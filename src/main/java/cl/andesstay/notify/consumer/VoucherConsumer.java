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

    private static final Set<String> DOCUMENTOS = Set.of("VOUCHER", "BOLETA");

    public VoucherConsumer(
            @Value("${andesstay.rabbitmq.queues.voucher}") String cola,
            @Value("${andesstay.rabbitmq.queues.voucher-retry}") String colaDeEspera,
            EnvelopeReader envelopeReader,
            ProcesamientoIdempotente procesamiento,
            ReintentoPublisher reintentos) {
        super(cola, colaDeEspera, "Voucher", envelopeReader, procesamiento, reintentos);
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.voucher}", ackMode = "MANUAL")
    public void processVoucher(Message message, Channel channel) {
        resolver(message, channel);
    }

    @Override
    protected void procesar(Envelope envelope, String traceId) {
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
