package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.HousekeepingService;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static cl.andesstay.notify.messaging.ErroresDeMensajeria.permanenteComoRechazo;

/**
 * Consume la cola de housekeeping (andesstay.rabbitmq.queues.housekeeping) y delega la emisión del
 * ticket en {@link HousekeepingService}.
 *
 * Solo hace mensajería. Sobre el manejo de errores y el modo de acknowledge, ver la explicación en
 * {@link EmailConsumer}: el método no captura excepciones, para que la política de reintentos tenga
 * efecto, y el modo AUTO es el que convierte un fallo agotado en un viaje a la DLQ.
 */
@Component
@RequiredArgsConstructor
public class HousekeepingConsumer {

    /** Nombre de la cola, para las métricas y el log: la misma propiedad que usa el listener. */
    @Value("${andesstay.rabbitmq.queues.housekeeping}")
    private String cola;

    private final EnvelopeReader envelopeReader;
    private final ProcesamientoIdempotente procesamiento;
    private final HousekeepingService housekeepingService;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping}")
    public void processHousekeeping(Message message) {
        Envelope envelope = envelopeReader.leer(message);
        String traceId = envelope.traceIdODesconocido();

        procesamiento.ejecutar(cola, envelope.eventId(), traceId,
                permanenteComoRechazo(() -> housekeepingService.emitir(aTicket(envelope), traceId)));
    }

    /** El formato contratado usa tipoTarea/fechaRequerida; el anterior usaba action/date. */
    static HousekeepingService.Ticket aTicket(Envelope envelope) {
        return new HousekeepingService.Ticket(
                envelope.texto("tipoTarea", "action"),
                envelope.texto("fechaRequerida", "date"),
                envelope.valor("unitId"),
                envelope.valor("reservationId"),
                envelope.texto("unidad"),
                envelope.texto("prioridad"),
                envelope.texto("notas"));
    }
}
