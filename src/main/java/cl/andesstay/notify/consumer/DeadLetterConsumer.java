package cl.andesstay.notify.consumer;

import cl.andesstay.notify.support.NotifyMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Observa las tres DLQ. No reprocesa nada: registra el mensaje y el motivo real del descarte, y
 * alimenta la métrica notify.messages.dlq.
 *
 * Sin este consumidor los mensajes muertos se acumulan sin que nadie los vea, que es la situación
 * que la guía 2.2.3 resuelve con su consumidor de cuarentena.
 *
 * Aquí sí se capturan todas las excepciones, al contrario que en los consumidores de trabajo. El
 * motivo es concreto: las DLQ no tienen dead letter propio, son el final del camino. Un rechazo
 * desde aquí descartaría el mensaje de forma definitiva, y perder la evidencia de un fallo es peor
 * que no poder formatear su log.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterConsumer {

    private final NotifyMetrics metrics;

    // Los nombres de las DLQ, para las métricas y el log: las mismas propiedades que usan los
    // listeners de abajo.
    @Value("${andesstay.rabbitmq.queues.email-dlq}")
    private String dlqEmail;
    @Value("${andesstay.rabbitmq.queues.housekeeping-dlq}")
    private String dlqHousekeeping;
    @Value("${andesstay.rabbitmq.queues.voucher-dlq}")
    private String dlqVoucher;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email-dlq}")
    public void emailMuerto(Message message) {
        registrar(dlqEmail, message);
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping-dlq}")
    public void housekeepingMuerto(Message message) {
        registrar(dlqHousekeeping, message);
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher-dlq}")
    public void voucherMuerto(Message message) {
        registrar(dlqVoucher, message);
    }

    private void registrar(String dlq, Message message) {
        metrics.enviadoALaDlq(dlq);
        try {
            log.error("[DLQ] Mensaje en cuarentena en {} · {} · cuerpo={}",
                    dlq, resumirXDeath(message), cuerpo(message));
        } catch (Exception e) {
            // Ver el comentario de clase: un fallo al formatear no debe costar el mensaje.
            log.error("[DLQ] Mensaje en cuarentena en {}, y además falló su registro: {}",
                    dlq, e.getMessage());
        }
    }

    /**
     * Resume la cabecera x-death, que es donde RabbitMQ deja el motivo del descarte.
     *
     * Los tres valores que interesan de {@code reason}: {@code rejected} cuando el consumidor
     * agotó los reintentos o rechazó de entrada, {@code expired} cuando venció el
     * {@code x-message-ttl}, y {@code maxlen} cuando la cola desbordó el {@code x-max-length}.
     */
    @SuppressWarnings("unchecked")
    private String resumirXDeath(Message message) {
        Object xDeath = message.getMessageProperties().getHeader("x-death");
        if (!(xDeath instanceof List<?> muertes) || muertes.isEmpty()) {
            return "sin cabecera x-death";
        }
        Object primera = muertes.get(0);
        if (!(primera instanceof Map<?, ?> mapa)) {
            return "x-death con formato inesperado";
        }
        Map<String, Object> muerte = (Map<String, Object>) mapa;
        return "motivo=%s colaOrigen=%s routingKeys=%s intentos=%s".formatted(
                muerte.get("reason"),
                muerte.get("queue"),
                muerte.get("routing-keys"),
                muerte.get("count"));
    }

    private String cuerpo(Message message) {
        byte[] bytes = message.getBody();
        if (bytes == null || bytes.length == 0) {
            return "(vacío)";
        }
        // Se recorta: un payload grande en el log no aporta sobre los primeros kilobytes y el
        // mensaje completo sigue disponible en la Management UI.
        int limite = Math.min(bytes.length, 2048);
        String texto = new String(bytes, 0, limite, StandardCharsets.UTF_8);
        return bytes.length > limite ? texto + "…(recortado)" : texto;
    }
}
