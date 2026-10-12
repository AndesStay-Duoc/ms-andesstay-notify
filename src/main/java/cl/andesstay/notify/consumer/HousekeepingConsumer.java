package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Consume q.cmd.housekeeping y emite el ticket de preparación o de limpieza de la unidad.
 *
 * Es el mensaje que resuelve el problema de que housekeeping se enteraba tarde.
 *
 * Sobre el manejo de errores y el modo de acknowledge, ver la explicación en
 * {@link EmailConsumer}: el método no captura excepciones, para que la política de reintentos
 * tenga efecto, y el modo AUTO es el que convierte un fallo agotado en un viaje a la DLQ.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HousekeepingConsumer {

    /** Nombre de la cola, para las métricas y el log: la misma propiedad que usa el listener. */
    @Value("${andesstay.rabbitmq.queues.housekeeping}")
    private String cola;

    /** PREPARACION al pasar a CHECKIN_PENDIENTE, LIMPIEZA al pasar a CHECKOUT. */
    private static final Set<String> TAREAS = Set.of("PREPARACION", "LIMPIEZA", "PREPARE", "CLEAN");

    private final EnvelopeReader envelopeReader;
    private final ProcesamientoIdempotente procesamiento;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping}")
    public void processHousekeeping(Message message) {
        Envelope envelope = envelopeReader.leer(message);
        String traceId = envelope.traceIdODesconocido();

        procesamiento.ejecutar(cola, envelope.eventId(), traceId, () -> emitirTicket(envelope, traceId));
    }

    private void emitirTicket(Envelope envelope, String traceId) {
        // El formato contratado usa tipoTarea/fechaRequerida; el anterior usaba action/date.
        String tipoTarea = envelope.texto("tipoTarea", "action");
        String fecha = envelope.texto("fechaRequerida", "date");
        Object unitId = envelope.valor("unitId");
        Object reservationId = envelope.valor("reservationId");
        String unidad = envelope.texto("unidad");
        String prioridad = envelope.texto("prioridad");
        String notas = envelope.texto("notas");

        if (tipoTarea == null || !TAREAS.contains(tipoTarea)) {
            throw new AmqpRejectAndDontRequeueException(
                    "tipoTarea desconocido: " + tipoTarea + " (conocidos: " + TAREAS + ")");
        }
        if (unitId == null) {
            throw new AmqpRejectAndDontRequeueException("el payload no trae unitId");
        }

        // TODO: integrar con el sistema de tickets del hostal o enviar push al equipo. Hasta
        // entonces el ticket queda en el log con todos sus datos, que es lo que consume la
        // evidencia de la fase 6.
        log.info("[Housekeeping] TICKET {} → unitId={} unidad={} reserva={} fecha={} prioridad={} notas={} traceId={}",
                tipoTarea, unitId, unidad, reservationId, fecha, prioridad, notas, traceId);
    }
}
