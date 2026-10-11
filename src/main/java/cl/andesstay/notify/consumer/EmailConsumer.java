package cl.andesstay.notify.consumer;

import cl.andesstay.notify.email.EmailTemplates;
import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/**
 * Consume q.cmd.email y envía el correo al huésped.
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
 * a q.cmd.email.dlq por el x-dead-letter-exchange de la cola.
 *
 * El AUTO de Spring no es el auto-ack del protocolo AMQP, que es AcknowledgeMode.NONE y sí pierde
 * mensajes si el consumidor muere. El AUTO emite un basic.ack real después de que el método
 * retorna.
 *
 * Reparto de responsabilidades entre los dos tipos de error:
 *
 *   transitorio (servidor SMTP caído, timeout)  -> se propaga tal cual, se reintenta
 *   permanente  (destinatario o plantilla malos) -> AmqpRejectAndDontRequeueException, a la DLQ ya
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EmailConsumer {

    private static final String COLA = "q.cmd.email";

    private final JavaMailSender mailSender;
    private final EnvelopeReader envelopeReader;
    private final EmailTemplates templates;
    private final ProcesamientoIdempotente procesamiento;

    @Value("${andesstay.mail.from}")
    private String fromAddress;

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email}")
    public void processEmail(Message message) {
        Envelope envelope = envelopeReader.leer(message);
        String traceId = envelope.traceIdODesconocido();

        procesamiento.ejecutar(COLA, envelope.eventId(), traceId, () -> {
            String destinatario = direccionValida(envelope);
            EmailTemplates.Correo correo = construir(envelope);

            log.info("[Email] Enviando a={} asunto='{}' traceId={}",
                    destinatario, correo.asunto(), traceId);

            SimpleMailMessage mail = new SimpleMailMessage();
            mail.setFrom(fromAddress);
            mail.setTo(destinatario);
            mail.setSubject(correo.asunto());
            mail.setText(correo.cuerpo());

            // Cualquier MailException sale de aquí sin tocarse: es el caso transitorio y lo tiene
            // que ver el interceptor de reintentos.
            mailSender.send(mail);

            log.info("[Email] Enviado correctamente traceId={}", traceId);
        });
    }

    /**
     * Resuelve el cuerpo del correo.
     *
     * El formato contratado trae {@code plantilla} y {@code datos}; el anterior traía
     * {@code subject} y {@code body} ya armados. Se aceptan los dos porque la publicación migra al
     * formato nuevo en un despliegue posterior al de este servicio.
     */
    private EmailTemplates.Correo construir(Envelope envelope) {
        String plantilla = envelope.texto("plantilla");
        if (plantilla != null) {
            Object reservationId = envelope.valor("reservationId");
            return templates.render(plantilla,
                    reservationId == null ? null : reservationId.toString(),
                    envelope.datos());
        }

        String asunto = envelope.texto("subject", "asunto");
        String cuerpo = envelope.texto("body", "cuerpo");
        if (asunto == null || cuerpo == null) {
            throw new AmqpRejectAndDontRequeueException(
                    "el payload no trae ni plantilla ni asunto/cuerpo: " + envelope.payload());
        }
        return new EmailTemplates.Correo(asunto, cuerpo);
    }

    /**
     * Un destinatario ausente o mal formado no se arregla reintentando, así que se clasifica como
     * permanente antes de molestar al servidor de correo.
     */
    private String direccionValida(Envelope envelope) {
        String destinatario = envelope.texto("destinatario", "to");
        if (destinatario == null) {
            throw new AmqpRejectAndDontRequeueException("el payload no trae destinatario");
        }
        try {
            new InternetAddress(destinatario).validate();
        } catch (AddressException e) {
            throw new AmqpRejectAndDontRequeueException(
                    "destinatario inválido '" + destinatario + "': " + e.getMessage());
        }
        return destinatario;
    }
}
