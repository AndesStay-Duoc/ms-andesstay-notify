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
public class VoucherConsumer {

    private final ObjectMapper objectMapper;

    /**
     * Consume mensajes de la cola q.cmd.voucher.
     * Genera un voucher PDF con los datos de la reserva y lo envía al huésped.
     * En producción se utilizaría una librería como iText o JasperReports.
     */
    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher}", ackMode = "MANUAL")
    public void processVoucher(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String traceId = "(sin traceId)";
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> envelope = objectMapper.readValue(message.getBody(), Map.class);
            traceId = (String) envelope.getOrDefault("traceId", traceId);

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
            Object reservationId = payload.get("reservationId");
            String guestEmail    = (String) payload.get("guestEmail");

            log.info("[Voucher] Generando voucher reserva={} para={} traceId={}",
                    reservationId, guestEmail, traceId);

            // TODO: Generar PDF con iText/JasperReports y adjuntarlo al email
            log.info("[Voucher] Voucher generado y enviado a {} (simulado)", guestEmail);

            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("[Voucher] Error generando voucher traceId={}: {}", traceId, e.getMessage());
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
