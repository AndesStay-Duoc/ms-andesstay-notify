package cl.andesstay.notify.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import java.net.ConnectException;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * La lectura de las nueve colas que antes hacia NotifyStatusController.
 *
 * Se prueba con AmqpAdmin simulado: lo que importa aqui es la forma de la respuesta y como se
 * reflejan los tres casos posibles —la cola responde, no existe, o el broker no contesta— y no la
 * conversacion con RabbitMQ.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EstadoDeColasServiceTest {

    @Mock
    private AmqpAdmin amqpAdmin;

    private EstadoDeColasService servicio;

    @BeforeEach
    void preparar() {
        servicio = new EstadoDeColasService(amqpAdmin,
                "q.cmd.email", "q.cmd.email.retry", "q.cmd.email.dlq",
                "q.cmd.housekeeping", "q.cmd.housekeeping.retry", "q.cmd.housekeeping.dlq",
                "q.cmd.voucher", "q.cmd.voucher.retry", "q.cmd.voucher.dlq");
        when(amqpAdmin.getQueueProperties(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(propiedades(0, 1));
    }

    @Test
    @DisplayName("tres flujos en orden, cada uno con trabajo, espera y cuarentena")
    void forma() {
        Map<String, Object> flujos = servicio.flujos();

        assertThat(flujos).containsOnlyKeys("email", "housekeeping", "voucher");
        assertThat(flujos.keySet()).containsExactly("email", "housekeeping", "voucher");
        assertThat(flujo(flujos, "email").keySet()).containsExactly("trabajo", "espera", "cuarentena");
        assertThat(flujo(flujos, "voucher").get("cuarentena")).isEqualTo(
                Map.of("q.cmd.voucher.dlq", Map.of("mensajes", 0, "consumidores", 1)));
    }

    @Test
    @DisplayName("una cola que responde muestra mensajes y consumidores")
    void conConteo() {
        when(amqpAdmin.getQueueProperties("q.cmd.email")).thenReturn(propiedades(7, 3));

        assertThat(flujo(servicio.flujos(), "email").get("trabajo"))
                .isEqualTo(Map.of("q.cmd.email", Map.of("mensajes", 7, "consumidores", 3)));
    }

    @Test
    @DisplayName("una cola inexistente se dice, en vez de mostrar un 0 engañoso")
    void inexistente() {
        when(amqpAdmin.getQueueProperties("q.cmd.email.retry")).thenReturn(null);

        assertThat(flujo(servicio.flujos(), "email").get("espera"))
                .isEqualTo(Map.of("q.cmd.email.retry", "no existe en el broker"));
    }

    @Test
    @DisplayName("un broker caido no tumba el estado: la cola sale como no consultable")
    void brokerCaido() {
        when(amqpAdmin.getQueueProperties("q.cmd.housekeeping"))
                .thenThrow(new AmqpConnectException(new ConnectException("Connection refused")));

        Object trabajo = flujo(servicio.flujos(), "housekeeping").get("trabajo");

        assertThat(trabajo).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) trabajo).get("q.cmd.housekeeping").toString())
                .startsWith("no consultable:");
    }

    // ── Apoyo ────────────────────────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static Map<String, Object> flujo(Map<String, Object> flujos, String nombre) {
        return (Map<String, Object>) flujos.get(nombre);
    }

    private static Properties propiedades(int mensajes, int consumidores) {
        Properties p = new Properties();
        p.put(RabbitAdmin.QUEUE_MESSAGE_COUNT, mensajes);
        p.put(RabbitAdmin.QUEUE_CONSUMER_COUNT, consumidores);
        return p;
    }
}
