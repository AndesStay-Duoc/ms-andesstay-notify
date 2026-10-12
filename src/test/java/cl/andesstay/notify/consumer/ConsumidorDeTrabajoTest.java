package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.NotifyMetrics;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import cl.andesstay.notify.support.ProcessedEventCache;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.mail.MailSendException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Los cinco caminos del ACK manual.
 *
 * Es la lógica que no se puede comprobar leyendo el código: cada camino termina en una llamada
 * distinta sobre el canal, y confundir dos de ellas produce un fallo que no se nota. El peor es el
 * camino 4 contra el 2: si el reintento viera su propio intento anterior como duplicado, haría
 * basicAck, el correo no se enviaría nunca, no quedaría nada en la DLQ y notify.dlq.rate seguiría
 * en cero. Un fallo silencioso con las métricas en verde.
 *
 * Se usan los colaboradores REALES —el registro de idempotencia, las métricas y el publicador de
 * reintentos— y se simula solo el canal. Lo que se está probando es cómo encajan entre sí, y con
 * mocks de todos ellos el test afirmaría su propia configuración.
 */
@ExtendWith(MockitoExtension.class)
class ConsumidorDeTrabajoTest {

    private static final String COLA = "q.cmd.email";
    private static final String COLA_ESPERA = "q.cmd.email.retry";
    private static final long TAG = 7L;

    /** Tres esperas, cuatro intentos. */
    private static final List<Long> ESCALERA = List.of(1000L, 2000L, 4000L);

    @Mock
    private Channel canal;

    private SimpleMeterRegistry registro;
    private ReintentoPublisher reintentos;
    private Consumidor consumidor;

    /** Lo que el trabajo de negocio hará en la siguiente entrega. */
    private Runnable trabajo;

    /** Cuántas veces se ejecutó de verdad el efecto. */
    private AtomicInteger ejecuciones;

    @BeforeEach
    void preparar() {
        registro = new SimpleMeterRegistry();
        ejecuciones = new AtomicInteger();
        trabajo = () -> {
        };

        ProcessedEventCache cache = new ProcessedEventCache(Duration.ofHours(6), 50_000);
        NotifyMetrics metrics = new NotifyMetrics(registro);
        ProcesamientoIdempotente procesamiento = new ProcesamientoIdempotente(cache, metrics);
        reintentos = new ReintentoPublisher(ESCALERA);
        EnvelopeReader lector = new EnvelopeReader(new ObjectMapper());

        consumidor = new Consumidor(lector, procesamiento, reintentos);
    }

    /** Consumidor mínimo: solo aporta el trabajo de negocio, que es lo que varía por flujo. */
    private final class Consumidor extends ConsumidorDeTrabajo {
        Consumidor(EnvelopeReader lector, ProcesamientoIdempotente procesamiento,
                   ReintentoPublisher reintentos) {
            super(COLA, COLA_ESPERA, "Prueba", lector, procesamiento, reintentos);
        }

        @Override
        protected void procesar(Envelope envelope, String traceId) {
            ejecuciones.incrementAndGet();
            trabajo.run();
        }

        void entregar(Message mensaje) {
            resolver(mensaje, canal);
        }
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────

    private static Message mensaje(String eventId, Integer intentosPrevios) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        if (intentosPrevios != null) {
            props.setHeader(ReintentoPublisher.HEADER_INTENTOS, intentosPrevios);
        }
        String cuerpo = """
                {"type":"EMAIL","eventId":"%s","traceId":"tr-1",
                 "payload":{"destinatario":"huesped@example.cl","plantilla":"RESERVA_CONFIRMADA"}}
                """.formatted(eventId);
        return new Message(cuerpo.getBytes(StandardCharsets.UTF_8), props);
    }

    private double contador(String nombre) {
        try {
            return registro.get(nombre).tag("queue", COLA).counter().count();
        } catch (MeterNotFoundException e) {
            // Micrometer no registra el contador hasta el primer incremento, así que no existir
            // equivale a cero.
            return 0d;
        }
    }

    // ── Camino 1 · éxito ─────────────────────────────────────────────────────

    @Test
    @DisplayName("1 · éxito: basicAck, y nada más")
    void exitoConfirma() throws Exception {
        consumidor.entregar(mensaje("E-1", null));

        verify(canal).basicAck(TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        verify(canal, never()).basicPublish(anyString(), anyString(), any(), any());
        assertEquals(1, ejecuciones.get());
        assertEquals(1d, contador("notify.messages.processed"));
    }

    // ── Camino 2 · duplicado ─────────────────────────────────────────────────

    @Test
    @DisplayName("2 · duplicado: dos basicAck, un solo efecto")
    void duplicadoConfirmaSinReejecutar() throws Exception {
        consumidor.entregar(mensaje("E-2", null));
        consumidor.entregar(mensaje("E-2", null));

        // Las dos entregas se confirman: la segunda es la garantía «al menos una vez» del broker
        // funcionando como debe, no un fallo.
        verify(canal, org.mockito.Mockito.times(2)).basicAck(TAG, false);
        assertEquals(1, ejecuciones.get(), "el efecto no se puede ejecutar dos veces");
        assertEquals(1d, contador("notify.messages.processed"));
    }

    // ── Camino 3 · error permanente ──────────────────────────────────────────

    @Test
    @DisplayName("3 · permanente: basicNack sin requeue en el PRIMER intento, sin pasar por la espera")
    void permanenteVaDirectoALaDlq() throws Exception {
        trabajo = () -> {
            throw new AmqpRejectAndDontRequeueException("destinatario inválido");
        };

        consumidor.entregar(mensaje("E-3", null));

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
        // Es la ruta diferenciada que el caso pide: un error que no se arregla reintentando no
        // gasta ni un intento ni toca la cola de espera.
        verify(canal, never()).basicPublish(anyString(), anyString(), any(), any());
        assertEquals(0d, contador("notify.messages.retried"));
    }

    @Test
    @DisplayName("3 bis · un cuerpo ilegible también es permanente, aunque falle antes del trabajo")
    void cuerpoIlegibleEsPermanente() throws Exception {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        Message roto = new Message("esto no es JSON".getBytes(StandardCharsets.UTF_8), props);

        consumidor.entregar(roto);

        // El lector del envelope se invoca DENTRO del try, así que su excepción permanente sale
        // por el camino 3 y no por la red de seguridad del catch(Throwable).
        verify(canal).basicNack(TAG, false, false);
        assertEquals(0, ejecuciones.get());
    }

    // ── Camino 4 · transitorio con reintento pendiente ───────────────────────

    @Test
    @DisplayName("4 · transitorio: publica en la cola de espera y DESPUÉS confirma el original")
    void transitorioProgramaYConfirma() throws Exception {
        trabajo = () -> {
            throw new MailSendException("SMTP caído");
        };

        consumidor.entregar(mensaje("E-4", null));

        ArgumentCaptor<String> cola = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<AMQP.BasicProperties> props =
                ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(canal).basicPublish(eq(""), cola.capture(), props.capture(), any());

        assertEquals(COLA_ESPERA, cola.getValue());
        // El exchange por omisión mantiene un binding implícito a cada cola por su nombre, y es lo
        // que permite que la topología siga siendo de tres exchanges y nueve bindings.
        assertEquals("1000", props.getValue().getExpiration(), "la espera del primer peldaño");
        assertEquals(1, props.getValue().getHeaders().get(ReintentoPublisher.HEADER_INTENTOS));
        assertEquals(2, props.getValue().getDeliveryMode(), "la copia tiene que ser persistente");

        verify(canal).basicAck(TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    @DisplayName("4 bis · si la publicación del reintento falla, el original NO se confirma")
    void siNoSePudoProgramarNoSeConfirma() throws Exception {
        trabajo = () -> {
            throw new MailSendException("SMTP caído");
        };
        doThrow(new IOException("canal roto"))
                .when(canal).basicPublish(anyString(), anyString(), any(), any());

        consumidor.entregar(mensaje("E-5", null));

        // Dejarlo sin confirmar es lo correcto: el broker lo reentregará con su cabecera de
        // intentos intacta. Confirmarlo habría perdido el mensaje, porque la copia no llegó.
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    @DisplayName("4 ter · el reintento NO se ve como duplicado de sí mismo")
    void elReintentoNoEsSuPropioDuplicado() throws Exception {
        // El fallo silencioso que este diseño tiene que evitar. El mensaje que vuelve de la cola
        // de espera trae el MISMO eventId; si la reserva de idempotencia no se liberase al fallar,
        // la segunda entrega tomaría el camino 2, confirmaría, y el fallo desaparecería sin dejar
        // nada en la DLQ ni en las métricas.
        trabajo = () -> {
            throw new MailSendException("SMTP caído");
        };
        consumidor.entregar(mensaje("E-6", null));

        trabajo = () -> {
        };
        consumidor.entregar(mensaje("E-6", 1));

        assertEquals(2, ejecuciones.get(),
                "el segundo intento tiene que EJECUTARSE, no descartarse como duplicado");
        assertEquals(1d, contador("notify.messages.processed"));
    }

    @Test
    @DisplayName("la escalera de esperas sube peldaño a peldaño")
    void laEscaleraSube() throws Exception {
        trabajo = () -> {
            throw new MailSendException("SMTP caído");
        };

        for (int previos = 0; previos < ESCALERA.size(); previos++) {
            consumidor.entregar(mensaje("E-7-" + previos, previos == 0 ? null : previos));
        }

        ArgumentCaptor<AMQP.BasicProperties> props =
                ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(canal, org.mockito.Mockito.times(3))
                .basicPublish(eq(""), eq(COLA_ESPERA), props.capture(), any());

        assertEquals(List.of("1000", "2000", "4000"),
                props.getAllValues().stream().map(AMQP.BasicProperties::getExpiration).toList());
    }

    // ── Camino 5 · transitorio agotado ───────────────────────────────────────

    @Test
    @DisplayName("5 · agotados los cuatro intentos: basicNack sin requeue, no otra espera")
    void agotadoVaALaDlq() throws Exception {
        trabajo = () -> {
            throw new MailSendException("SMTP caído");
        };

        // Tres esperas significan cuatro intentos, así que el cuarto es el último.
        consumidor.entregar(mensaje("E-8", 3));

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicPublish(anyString(), anyString(), any(), any());
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
    }

    @Test
    @DisplayName("un intento por encima del máximo tampoco programa otra espera")
    void porEncimaDelMaximoTampocoReintenta() throws Exception {
        trabajo = () -> {
            throw new MailSendException("SMTP caído");
        };

        consumidor.entregar(mensaje("E-9", 9));

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicPublish(anyString(), anyString(), any(), any());
    }

    // ── Red de seguridad ─────────────────────────────────────────────────────

    @Test
    @DisplayName("un Error inesperado rechaza el mensaje ANTES de propagarse")
    void errorInesperadoRechazaYSube() throws Exception {
        trabajo = () -> {
            throw new NoClassDefFoundError("falta una clase");
        };

        assertThrows(NoClassDefFoundError.class, () -> consumidor.entregar(mensaje("E-10", null)));

        // Sin esto, con ackMode MANUAL el mensaje quedaría colgado hasta el cierre del canal y
        // entonces el broker lo REENCOLA: un bucle de reentrega sin backoff y sin límite.
        verify(canal).basicNack(TAG, false, false);
    }

    // ── La métrica que se apagaría en silencio ───────────────────────────────

    @Test
    @DisplayName("notify.messages.retried se mueve de verdad al reintentar")
    void laMetricaDeReintentosSeMueve() {
        // Es la comprobación que detecta el olvido más peligroso del rediseño. La versión anterior
        // leía el intento del RetryContext de Spring Retry; sin ese interceptor devolvería siempre
        // null, la condición nunca se cumpliría y este contador se quedaría clavado en cero sin
        // que nada más fallara.
        consumidor.entregar(mensaje("E-11", null));
        assertEquals(0d, contador("notify.messages.retried"), "el primer intento no es un reintento");

        consumidor.entregar(mensaje("E-12", 1));
        consumidor.entregar(mensaje("E-13", 2));

        assertEquals(2d, contador("notify.messages.retried"));
    }

    @Test
    @DisplayName("la tasa de DLQ sigue en cero mientras no muera ningún mensaje")
    void laTasaDeDlqNoSeEnsucia() {
        consumidor.entregar(mensaje("E-14", null));

        // El consumidor de trabajo no toca notify.messages.dlq: eso lo hace DeadLetterConsumer al
        // ver el mensaje llegar a la cuarentena. Así la tasa mide lo que de verdad murió y no lo
        // que se intentó rechazar.
        assertTrue(registro.find("notify.messages.dlq").counters().isEmpty());
    }
}
