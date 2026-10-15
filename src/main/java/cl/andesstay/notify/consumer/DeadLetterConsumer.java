package cl.andesstay.notify.consumer;

import cl.andesstay.notify.support.NotifyMetrics;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
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
 * ACK MANUAL CON LA CONFIRMACIÓN EN UN finally, y el orden importa. Aquí se capturan todas las
 * excepciones, al contrario que en los consumidores de trabajo, por un motivo concreto: las DLQ no
 * tienen dead letter propio, son el final del camino. Un rechazo desde aquí descartaría el mensaje
 * de forma definitiva, y perder la evidencia de un fallo es peor que no poder formatear su log. Por
 * eso el basicAck va en un finally y nunca en un catch que pudiera no ejecutarse: el mensaje se
 * confirma pase lo que pase, porque lo único que hace este consumidor es mirarlo.
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

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email-dlq}", ackMode = "MANUAL")
    public void emailMuerto(Message message, Channel channel) {
        registrar(dlqEmail, message, channel);
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping-dlq}", ackMode = "MANUAL")
    public void housekeepingMuerto(Message message, Channel channel) {
        registrar(dlqHousekeeping, message, channel);
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher-dlq}", ackMode = "MANUAL")
    public void voucherMuerto(Message message, Channel channel) {
        registrar(dlqVoucher, message, channel);
    }

    private void registrar(String dlq, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            metrics.enviadoALaDlq(dlq);
            log.error("[DLQ] Mensaje en cuarentena en {} · {} · {} · cuerpo={}",
                    dlq, resumirXDeath(message), intentosConsumidos(message), cuerpo(message));
        } catch (Exception e) {
            // Ver el comentario de clase: un fallo al formatear no debe costar el mensaje.
            log.error("[DLQ] Mensaje en cuarentena en {}, y además falló su registro: {}",
                    dlq, e.getMessage());
        } finally {
            try {
                channel.basicAck(deliveryTag, false);
            } catch (IOException e) {
                log.error("[DLQ] Falló el ACK en {} tag={}: {}. El broker lo reentregará y se "
                                + "volverá a registrar.", dlq, deliveryTag, e.getMessage());
            }
        }
    }

    /**
     * La cuenta de intentos que escribió el consumidor de trabajo.
     *
     * Es la prueba directa de que los reintentos se agotaron, y se lee de la cabecera propia y no
     * del {@code count} de x-death porque ese contador va por el par (cola, motivo) y no por
     * intento. Ausente significa que el mensaje llegó a la DLQ sin consumir ningún reintento, que
     * es exactamente lo que ocurre con un error permanente.
     */
    private String intentosConsumidos(Message message) {
        Object valor = message.getMessageProperties().getHeader(ReintentoPublisher.HEADER_INTENTOS);
        if (!(valor instanceof Number intentos)) {
            return "intentosConsumidos=0 (error permanente: no gastó reintentos)";
        }
        return "intentosConsumidos=" + intentos.intValue();
    }

    /**
     * Resume la cabecera x-death, que es donde RabbitMQ deja el motivo del descarte.
     *
     * Los tres valores que interesan de {@code reason}: {@code rejected} cuando el consumidor
     * agotó los reintentos o rechazó de entrada, {@code expired} cuando venció el
     * {@code x-message-ttl}, y {@code maxlen} cuando la cola desbordó el {@code x-max-length}.
     *
     * Se lee el índice 0, que es LA MUERTE MÁS RECIENTE y no la primera: RabbitMQ pone al
     * principio la última. Con esta topología solo hay una muerte posible por mensaje, porque
     * ReintentoPublisher descarta x-death al republicar en la cola de espera precisamente para que
     * la entrada de la DLQ quede limpia; la cuenta de reintentos la aporta la cabecera propia.
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
        return "motivo=%s colaOrigen=%s routingKeys=%s".formatted(
                muerte.get("reason"),
                muerte.get("queue"),
                muerte.get("routing-keys"));
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
