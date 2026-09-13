package cl.andesstay.notify.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class HousekeepingConsumer {

    private final ObjectMapper objectMapper;

    /**
     * Consume mensajes de la cola q.cmd.housekeeping.
     * En un sistema real aquí se enviaría una notificación push al equipo de housekeeping
     * o se crearía un ticket en el sistema de gestión del hostal.
     */
    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping}", ackMode = "MANUAL")
    public void processHousekeeping(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String traceId = "(sin traceId)";
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> envelope = objectMapper.readValue(message.getBody(), Map.class);
            traceId = (String) envelope.getOrDefault("traceId", traceId);

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
            Object unitId = payload.get("unitId");
            String date   = (String) payload.get("date");
            String action = (String) payload.get("action");  // PREPARE | CLEAN

            log.info("[Housekeeping] Ticket generado → unitId={} fecha={} acción={} traceId={}",
                    unitId, date, action, traceId);

            // TODO: Integrar con sistema de tickets del hostal o enviar push a staff
            // Por ahora loguea el ticket con todos sus datos
            log.info("[Housekeeping] TICKET: unidad={} debe ser {} para el {}",
                    unitId, action.equals("PREPARE") ? "preparada" : "limpiada", date);

            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[Housekeeping] Error procesando ticket traceId={}: {}", traceId, e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
