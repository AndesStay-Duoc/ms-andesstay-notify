package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.VoucherService;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static cl.andesstay.notify.messaging.ErroresDeMensajeria.permanenteComoRechazo;

/**
 * Consume la cola de comprobantes (andesstay.rabbitmq.queues.voucher) y delega la generación en
 * {@link VoucherService}.
 *
 * Solo hace mensajería. Sobre el manejo de errores y el modo de acknowledge, ver la explicación en
 * {@link EmailConsumer}.
 */
@Component
@RequiredArgsConstructor
public class VoucherConsumer {

    /** Nombre de la cola, para las métricas y el log: la misma propiedad que usa el listener. */
    @Value("${andesstay.rabbitmq.queues.voucher}")
    private String cola;

    private final EnvelopeReader envelopeReader;
    private final ProcesamientoIdempotente procesamiento;
    private final VoucherService voucherService;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher}")
    public void processVoucher(Message message) {
        Envelope envelope = envelopeReader.leer(message);
        String traceId = envelope.traceIdODesconocido();

        procesamiento.ejecutar(cola, envelope.eventId(), traceId,
                permanenteComoRechazo(() -> voucherService.generar(aSolicitud(envelope), traceId)));
    }

    /** El formato contratado usa destinatario/tipoDocumento; el anterior usaba guestEmail. */
    static VoucherService.Solicitud aSolicitud(Envelope envelope) {
        return new VoucherService.Solicitud(
                envelope.valor("reservationId"),
                envelope.texto("destinatario", "guestEmail"),
                envelope.texto("tipoDocumento"),
                envelope.datos());
    }
}
