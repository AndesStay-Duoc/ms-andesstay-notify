package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.HousekeepingService;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static cl.andesstay.notify.messaging.ErroresDeMensajeria.permanenteComoRechazo;

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

    private final HousekeepingService housekeepingService;

    public HousekeepingConsumer(
            @Value("${andesstay.rabbitmq.queues.housekeeping}") String cola,
            @Value("${andesstay.rabbitmq.queues.housekeeping-retry}") String colaDeEspera,
            HousekeepingService housekeepingService,
            EnvelopeReader envelopeReader,
            ProcesamientoIdempotente procesamiento,
            ReintentoPublisher reintentos) {
        super(cola, colaDeEspera, "Housekeeping", envelopeReader, procesamiento, reintentos);
        this.housekeepingService = housekeepingService;
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.housekeeping}", ackMode = "MANUAL")
    public void processHousekeeping(Message message, Channel channel) {
        resolver(message, channel);
    }

    /** Las validaciones y el ticket viven en HousekeepingService; un dato faltante es permanente. */
    @Override
    protected void procesar(Envelope envelope, String traceId) {
        permanenteComoRechazo(() -> housekeepingService.emitir(aTicket(envelope), traceId)).run();
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
