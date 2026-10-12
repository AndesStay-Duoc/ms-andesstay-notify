package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.CorreoService;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static cl.andesstay.notify.messaging.ErroresDeMensajeria.permanenteComoRechazo;

/**
 * Consume la cola de correos (andesstay.rabbitmq.queues.email) y delega el envío en
 * {@link CorreoService}.
 *
 * Este consumidor solo hace mensajería: lee el envelope, lo traduce a una solicitud, delega, y
 * deja que el contenedor confirme o rechace. La lógica del correo vive en el servicio.
 *
 * Sobre el manejo de errores, que es el núcleo de la fase 6: este método NO captura las
 * excepciones. Antes lo hacía, y eso dejaba inerte la política de reintentos, porque el
 * RetryOperationsInterceptor de Spring solo actúa cuando el método del listener lanza. Al capturar
 * y hacer basicNack, el contenedor veía un método que terminó bien y el mensaje iba a la DLQ en el
 * primer fallo, sin reintentos.
 *
 * Tampoco se usa ya el modo de acknowledge manual. Con AcknowledgeMode.MANUAL el contenedor no
 * envía ack ni nack por su cuenta, así que una excepción dejaría el mensaje sin confirmar hasta el
 * cierre del canal y entonces se REENCOLARÍA en lugar de ir a la DLQ. Con el modo AUTO, que es el
 * que queda por omisión, el contenedor confirma al retornar sin error y rechaza al propagarse la
 * excepción; agotados los reintentos, ese rechazo es un basic.reject sin requeue y el mensaje viaja
 * a la DLQ de correos por el x-dead-letter-exchange de la cola.
 *
 * El AUTO de Spring no es el auto-ack del protocolo AMQP, que es AcknowledgeMode.NONE y sí pierde
 * mensajes si el consumidor muere. El AUTO emite un basic.ack real después de que el método
 * retorna.
 *
 * Reparto de responsabilidades entre los dos tipos de error (ver ErroresDeMensajeria):
 *
 *   transitorio (servidor SMTP caído, timeout)  -> se propaga tal cual, se reintenta
 *   permanente  (destinatario o plantilla malos) -> AmqpRejectAndDontRequeueException, a la DLQ ya
 */
@Component
@RequiredArgsConstructor
public class EmailConsumer {

    /** Nombre de la cola, para las métricas y el log: la misma propiedad que usa el listener. */
    @Value("${andesstay.rabbitmq.queues.email}")
    private String cola;

    private final EnvelopeReader envelopeReader;
    private final ProcesamientoIdempotente procesamiento;
    private final CorreoService correoService;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email}")
    public void processEmail(Message message) {
        Envelope envelope = envelopeReader.leer(message);
        String traceId = envelope.traceIdODesconocido();

        procesamiento.ejecutar(cola, envelope.eventId(), traceId,
                permanenteComoRechazo(() -> correoService.enviar(aSolicitud(envelope), traceId)));
    }

    /**
     * Traduce el payload a la solicitud del servicio. Acepta los nombres del formato contratado
     * (destinatario, plantilla, datos) y los del anterior (to, subject, body).
     */
    static CorreoService.Solicitud aSolicitud(Envelope envelope) {
        Object reservationId = envelope.valor("reservationId");
        return new CorreoService.Solicitud(
                envelope.texto("destinatario", "to"),
                envelope.texto("plantilla"),
                reservationId == null ? null : reservationId.toString(),
                envelope.datos(),
                envelope.texto("subject", "asunto"),
                envelope.texto("body", "cuerpo"));
    }
}
