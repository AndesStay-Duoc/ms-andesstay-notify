package cl.andesstay.notify.support;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * La copia que se publica en la cola de espera.
 *
 * Cada aserción de aquí corresponde a una forma distinta de perder un mensaje en silencio, y
 * ninguna de ellas produce un error visible si se rompe:
 *
 *   `expiration` como entero         el broker la ignora y el mensaje se queda ahí para siempre
 *   delivery_mode no persistente     un reinicio del broker se lleva los reintentos pendientes
 *   x-death copiada                  la entrada de la DLQ deja de ser legible
 *   cabeceras de negocio perdidas    se va el eventId y con él la idempotencia
 */
@ExtendWith(MockitoExtension.class)
class ReintentoPublisherTest {

    private static final String COLA = "q.cmd.email";
    private static final String COLA_ESPERA = "q.cmd.email.retry";

    @Mock
    private Channel canal;

    private final ReintentoPublisher publicador =
            new ReintentoPublisher(List.of(1000L, 2000L, 4000L));

    /** Mensaje con cabeceras de negocio y con las que escribe el broker. */
    private static Message original() {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setContentEncoding("UTF-8");
        props.setMessageId("msg-1");
        props.setCorrelationId("corr-1");
        props.setType("EMAIL");
        props.setHeader("traceId", "tr-1");
        props.setHeader("x-death", List.of(Map.of("queue", COLA, "reason", "rejected", "count", 3L)));
        props.setHeader("x-first-death-queue", COLA);
        props.setHeader("x-first-death-reason", "rejected");
        return new Message("{}".getBytes(StandardCharsets.UTF_8), props);
    }

    private AMQP.BasicProperties publicar(int intento) throws Exception {
        publicador.programar(COLA, COLA_ESPERA, original(), intento, canal);
        ArgumentCaptor<AMQP.BasicProperties> props =
                ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(canal).basicPublish(eq(""), eq(COLA_ESPERA), props.capture(), any());
        return props.getValue();
    }

    // ── La escalera ──────────────────────────────────────────────────────────

    @Test
    @DisplayName("tres esperas significan CUATRO intentos")
    void tresEsperasSonCuatroIntentos() {
        // La aritmética que la configuración anterior tenía mal: max-attempts 3 con multiplicador
        // 2 daba tres intentos y DOS esperas (1 s y 2 s), así que el peldaño de 4 s que la
        // documentación prometía no llegaba a usarse nunca.
        assertEquals(4, publicador.maxIntentos());
        assertTrue(publicador.quedanIntentos(3), "tras el tercer intento queda el cuarto");
        assertFalse(publicador.quedanIntentos(4), "el cuarto es el último");
    }

    @ParameterizedTest
    @CsvSource({"1,1000", "2,2000", "3,4000"})
    @DisplayName("cada intento espera su peldaño")
    void cadaIntentoSuPeldano(int intento, long espera) {
        assertEquals(espera, publicador.esperaTrasIntento(intento));
    }

    @Test
    @DisplayName("por encima de la escalera se repite el último peldaño en lugar de desbordar")
    void porEncimaRepiteElUltimo() {
        // Nunca debería ocurrir —el consumidor comprueba quedanIntentos antes— pero un
        // IndexOutOfBounds aquí saldría por el catch(Throwable) y mandaría a la DLQ un mensaje
        // perfectamente reintentable.
        assertEquals(4000L, publicador.esperaTrasIntento(9));
    }

    @Test
    @DisplayName("una escalera vacía impide arrancar, en lugar de degradar a cero reintentos")
    void escaleraVaciaNoArranca() {
        // Con la lista vacía el servicio mandaría todo a la DLQ en el primer fallo sin que nada lo
        // advirtiera. Mejor no arrancar.
        assertThrows(IllegalArgumentException.class, () -> new ReintentoPublisher(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new ReintentoPublisher(null));
    }

    // ── La cuenta de intentos ────────────────────────────────────────────────

    @Nested
    @DisplayName("la cabecera de intentos")
    class Cabecera {

        @Test
        @DisplayName("ausente significa primera entrega")
        void ausenteEsLaPrimera() {
            assertEquals(1, publicador.intentoActual(new Message(new byte[0], new MessageProperties())));
        }

        /**
         * El entero puede llegar como Integer, Long o Short según cómo viajara por el protocolo,
         * de ahí que se compruebe contra Number y no contra Integer. Un cast a Integer habría
         * lanzado ClassCastException y el mensaje habría salido por la red de seguridad.
         */
        @Test
        @DisplayName("se lee igual venga como Integer, Long o Short")
        void cualquierNumeroVale() {
            for (Number valor : List.of(2, 2L, (short) 2)) {
                MessageProperties props = new MessageProperties();
                props.setHeader(ReintentoPublisher.HEADER_INTENTOS, valor);
                assertEquals(3, publicador.intentoActual(new Message(new byte[0], props)),
                        "con " + valor.getClass().getSimpleName());
            }
        }

        @Test
        @DisplayName("un valor que no es un número se trata como ausente")
        void valorRaroEsCeroIntentos() {
            MessageProperties props = new MessageProperties();
            props.setHeader(ReintentoPublisher.HEADER_INTENTOS, "tres");
            assertEquals(1, publicador.intentoActual(new Message(new byte[0], props)));
        }

        @Test
        @DisplayName("la copia la lleva incrementada")
        void laCopiaLaIncrementa() throws Exception {
            assertEquals(2, publicar(2).getHeaders().get(ReintentoPublisher.HEADER_INTENTOS));
        }
    }

    // ── Las propiedades de la copia ──────────────────────────────────────────

    @Test
    @DisplayName("expiration es una CADENA de milisegundos, no un entero")
    void expirationEsCadena() throws Exception {
        // El error clásico. AMQP define `expiration` como short string; con un entero el broker la
        // ignora y el mensaje se queda en la cola de espera indefinidamente, sin consumidor que lo
        // saque y sin que nada lo advierta.
        assertEquals("1000", publicar(1).getExpiration());
    }

    @Test
    @DisplayName("la copia es persistente")
    void laCopiaEsPersistente() throws Exception {
        // Es un mensaje NUEVO: si no se le pone delivery_mode 2, un reinicio del broker se lleva
        // todos los reintentos pendientes, que es justo el momento en que importan.
        assertEquals(2, publicar(1).getDeliveryMode());
    }

    @Test
    @DisplayName("se descartan x-death y x-first-death-*, que son del broker")
    void sinLasCabecerasDelBroker() throws Exception {
        Map<String, Object> cabeceras = publicar(1).getHeaders();

        // Copiarlas dejaría al vencimiento actualizando un historial que propagamos a mano, y haría
        // ilegible la entrada de la DLQ: su x-death[0] dejaría de ser la muerte que importa.
        assertNull(cabeceras.get("x-death"));
        assertTrue(cabeceras.keySet().stream().noneMatch(k -> k.startsWith("x-first-death")),
                "quedaron cabeceras del broker: " + cabeceras.keySet());
    }

    @Test
    @DisplayName("las cabeceras y los identificadores de negocio sí se conservan")
    void conservaLoDeNegocio() throws Exception {
        AMQP.BasicProperties copia = publicar(1);

        // Perder el messageId o el traceId rompería la correlación de los logs, y el eventId del
        // cuerpo es la clave de idempotencia: sin él, el reintento se procesaría dos veces.
        assertEquals("tr-1", copia.getHeaders().get("traceId"));
        assertEquals("msg-1", copia.getMessageId());
        assertEquals("corr-1", copia.getCorrelationId());
        assertEquals("EMAIL", copia.getType());
        assertEquals(MessageProperties.CONTENT_TYPE_JSON, copia.getContentType());
    }

    @Test
    @DisplayName("el cuerpo viaja intacto")
    void elCuerpoViajaIntacto() throws Exception {
        Message original = original();

        publicador.programar(COLA, COLA_ESPERA, original, 1, canal);

        ArgumentCaptor<byte[]> cuerpo = ArgumentCaptor.forClass(byte[].class);
        verify(canal).basicPublish(eq(""), eq(COLA_ESPERA), any(), cuerpo.capture());
        assertEquals("{}", new String(cuerpo.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("se publica por el exchange por omisión, con la cola como routing key")
    void publicaPorElExchangePorOmision() throws Exception {
        publicador.programar(COLA, COLA_ESPERA, original(), 1, canal);

        // Es lo que evita declarar un exchange y tres bindings, y por eso la topología sigue
        // siendo de tres exchanges y nueve bindings: el exchange por omisión mantiene un binding
        // implícito a cada cola por su nombre.
        verify(canal).basicPublish(eq(""), eq(COLA_ESPERA), any(), any());
    }
}
