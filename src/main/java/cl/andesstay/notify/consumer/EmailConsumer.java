package cl.andesstay.notify.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailConsumer {

    private final JavaMailSender mailSender;
    private final ObjectMapper objectMapper;

    @Value("${andesstay.mail.from}")
    private String fromAddress;

    /**
     * Consume mensajes de la cola q.cmd.email.
     * Envía el correo al huésped y da ACK explícito.
     * En caso de error reintenta hasta 3 veces; si falla, el mensaje va a la DLQ.
     */
    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email}", ackMode = "MANUAL")
    public void processEmail(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String traceId = "(sin traceId)";
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> envelope = objectMapper.readValue(message.getBody(), Map.class);
            traceId = (String) envelope.getOrDefault("traceId", traceId);

            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
            String to      = (String) payload.get("to");
            String subject = (String) payload.get("subject");
            String body    = (String) payload.get("body");

            log.info("[Email] Enviando a={} subject='{}' traceId={}", to, subject, traceId);

            SimpleMailMessage mail = new SimpleMailMessage();
            mail.setFrom(fromAddress);
            mail.setTo(to);
            mail.setSubject(subject);
            mail.setText(body);
            mailSender.send(mail);

            channel.basicAck(deliveryTag, false);
            log.info("[Email] Enviado correctamente traceId={}", traceId);

        } catch (Exception e) {
            log.error("[Email] Error procesando mensaje traceId={}: {}", traceId, e.getMessage());
            // NACK sin requeue → va a DLQ
            channel.basicNack(deliveryTag, false, false);
        }
    }
}
