package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.rabbitmq.client.Channel;
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
 * Gestiona su acknowledge de forma explícita, igual que {@link EmailConsumer}: ackMode MANUAL en la
 * anotación y el canal como parámetro. Los cinco caminos están en {@link ConsumidorDeTrabajo}.
 */
@Slf4j
@Component
public class HousekeepingConsumer extends ConsumidorDeTrabajo {

    /** PREPARACION al pasar a CHECKIN_PENDIENTE, LIMPIEZA al pasar a CHECKOUT. */
    private static final Set<String> TAREAS = Set.of("PREPARACION", "LIMPIEZA", "PREPARE", "CLEAN");

    public HousekeepingConsumer(
            @Value("${andesstay.rabbitmq.queues.housekeeping}") String cola,
            @Value("${andesstay.rabbitmq.queues.housekeeping-retry}") String colaDeEspera,
            EnvelopeReader envelopeReader,
            ProcesamientoIdempotente procesamiento,
            ReintentoPublisher reintentos) {
        super(cola, colaDeEspera, "Housekeeping", envelopeReader, procesamiento, reintentos);
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping}", ackMode = "MANUAL")
    public void processHousekeeping(Message message, Channel channel) {
        resolver(message, channel);
    }

    @Override
    protected void procesar(Envelope envelope, String traceId) {
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
