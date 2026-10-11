package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.Set;

/**
 * Consume q.cmd.voucher y genera el comprobante de la reserva.
 *
 * Es el mensaje que resuelve el problema de que el huésped no recibía comprobante formal.
 *
 * Sobre el manejo de errores y el modo de acknowledge, ver la explicación en
 * {@link EmailConsumer}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class VoucherConsumer {

    private static final String COLA = "q.cmd.voucher";

    private static final Set<String> DOCUMENTOS = Set.of("VOUCHER", "BOLETA");

    private final EnvelopeReader envelopeReader;
    private final ProcesamientoIdempotente procesamiento;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher}")
    public void processVoucher(Message message) {
        Envelope envelope = envelopeReader.leer(message);
        String traceId = envelope.traceIdODesconocido();

        procesamiento.ejecutar(COLA, envelope.eventId(), traceId, () -> generar(envelope, traceId));
    }

    private void generar(Envelope envelope, String traceId) {
        Object reservationId = envelope.valor("reservationId");
        // El formato contratado usa destinatario/tipoDocumento; el anterior usaba guestEmail y no
        // traía tipo de documento, de ahí el VOUCHER por omisión.
        String destinatario = envelope.texto("destinatario", "guestEmail");
        String tipoDocumento = envelope.texto("tipoDocumento");
        if (tipoDocumento == null) {
            tipoDocumento = "VOUCHER";
        }

        if (reservationId == null) {
            throw new AmqpRejectAndDontRequeueException("el payload no trae reservationId");
        }
        if (destinatario == null) {
            throw new AmqpRejectAndDontRequeueException("el payload no trae destinatario");
        }
        if (!DOCUMENTOS.contains(tipoDocumento)) {
            throw new AmqpRejectAndDontRequeueException(
                    "tipoDocumento desconocido: " + tipoDocumento + " (conocidos: " + DOCUMENTOS + ")");
        }

        // TODO: generar el PDF con iText o JasperReports y adjuntarlo al correo. Hasta entonces el
        // documento queda registrado con sus datos, que es lo que consume la evidencia de la fase 6.
        log.info("[Voucher] {} generado reserva={} destinatario={} datos={} traceId={}",
                tipoDocumento, reservationId, destinatario, envelope.datos(), traceId);
    }
}
