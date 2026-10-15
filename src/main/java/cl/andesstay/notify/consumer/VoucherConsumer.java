package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.VoucherService;
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
 * Consume q.cmd.voucher y genera el documento de la reserva.
 *
 * Es el mensaje que resuelve el problema de que el huésped no recibía un voucher formal.
 *
 * Gestiona su acknowledge de forma explícita, igual que {@link EmailConsumer}: ackMode MANUAL en la
 * anotación y el canal como parámetro. Los cinco caminos están en {@link ConsumidorDeTrabajo}.
 */
@Slf4j
@Component
public class VoucherConsumer extends ConsumidorDeTrabajo {

    private final VoucherService voucherService;

    public VoucherConsumer(
            @Value("${andesstay.rabbitmq.queues.voucher}") String cola,
            @Value("${andesstay.rabbitmq.queues.voucher-retry}") String colaDeEspera,
            VoucherService voucherService,
            EnvelopeReader envelopeReader,
            ProcesamientoIdempotente procesamiento,
            ReintentoPublisher reintentos) {
        super(cola, colaDeEspera, "Voucher", envelopeReader, procesamiento, reintentos);
        this.voucherService = voucherService;
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher}", ackMode = "MANUAL")
    public void processVoucher(Message message, Channel channel) {
        resolver(message, channel);
    }

    /** Las validaciones y la generación viven en VoucherService; un dato faltante es permanente. */
    @Override
    protected void procesar(Envelope envelope, String traceId) {
        permanenteComoRechazo(() -> voucherService.generar(aSolicitud(envelope), traceId)).run();
    }

    /**
     * El formato contratado usa destinatario/tipoDocumento; el anterior usaba guestEmail y no
     * traía tipo de documento, de ahí el VOUCHER por omisión que aplica el servicio.
     */
    static VoucherService.Solicitud aSolicitud(Envelope envelope) {
        return new VoucherService.Solicitud(
                envelope.valor("reservationId"),
                envelope.texto("destinatario", "guestEmail"),
                envelope.texto("tipoDocumento"),
                envelope.datos());
    }
}
