package cl.andesstay.notify.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;

/**
 * Convierte el cuerpo del mensaje en un {@link Envelope}.
 *
 * Un cuerpo ilegible o sin eventId es un error PERMANENTE: reintentarlo tres veces no lo va a
 * volver legible. Por eso se lanza {@link AmqpRejectAndDontRequeueException}, que el contenedor
 * traduce en un reject sin requeue y manda el mensaje a su DLQ de inmediato, sin gastar la política
 * de reintentos. Ver infra/docs/contracts/events/rabbit.md, sección de errores transitorios y
 * permanentes.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EnvelopeReader {

    private final ObjectMapper objectMapper;

    public Envelope leer(Message message) {
        Envelope envelope;
        try {
            envelope = objectMapper.readValue(message.getBody(), Envelope.class);
        } catch (Exception e) {
            throw permanente("cuerpo ilegible: " + e.getMessage(), message);
        }

        if (envelope == null) {
            throw permanente("cuerpo vacío", message);
        }
        if (envelope.eventId() == null || envelope.eventId().isBlank()) {
            // Sin eventId no hay clave de idempotencia, así que el mensaje no se puede procesar de
            // forma segura: una reentrega lo ejecutaría dos veces.
            throw permanente("envelope sin eventId", message);
        }
        return envelope;
    }

    private AmqpRejectAndDontRequeueException permanente(String motivo, Message message) {
        log.error("[Envelope] Error permanente, va directo a la DLQ: {} (cola de origen={})",
                motivo, message.getMessageProperties().getConsumerQueue());
        return new AmqpRejectAndDontRequeueException(motivo);
    }
}
