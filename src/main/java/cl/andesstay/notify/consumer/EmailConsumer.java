package cl.andesstay.notify.consumer;

import cl.andesstay.notify.email.EmailTemplates;
import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.rabbitmq.client.Channel;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
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
 * Gestiona su acknowledge de forma explícita: el ackMode MANUAL de la anotación hace que el
 * contenedor no confirme ni rechace nada, y el canal llega como parámetro para que sea este
 * consumidor quien emita el basicAck o el basicNack. Los cinco caminos están en
 * {@link ConsumidorDeTrabajo}, que es la clase que los resuelve.
 *
 * El ackMode va en la anotación y NO en el application.yml. Puesto en el YAML sería global y
 * alcanzaría también a DeadLetterConsumer, que no confirma nada por su cuenta: las tres DLQ se
 * llenarían de mensajes en estado unacked que se reencolarían al cerrar el canal.
 *
 * Reparto de responsabilidades entre los dos tipos de error:
 *
 *   transitorio (servidor SMTP caído, timeout)   -> se propaga tal cual, se reintenta
 *   permanente  (destinatario o plantilla malos) -> AmqpRejectAndDontRequeueException, a la DLQ ya
 */
@Slf4j
@Component
public class EmailConsumer extends ConsumidorDeTrabajo {

    private final JavaMailSender mailSender;
    private final EmailTemplates templates;
    private final String fromAddress;

    public EmailConsumer(
            @Value("${andesstay.rabbitmq.queues.email}") String cola,
            @Value("${andesstay.rabbitmq.queues.email-retry}") String colaDeEspera,
            @Value("${andesstay.mail.from}") String fromAddress,
            JavaMailSender mailSender,
            EmailTemplates templates,
            EnvelopeReader envelopeReader,
            ProcesamientoIdempotente procesamiento,
            ReintentoPublisher reintentos) {
        // El nombre de la cola sale de la MISMA propiedad que el @RabbitListener de abajo, en
        // lugar de una constante aparte: así la etiqueta de las métricas no puede desincronizarse
        // de la cola que realmente se escucha.
        super(cola, colaDeEspera, "Email", envelopeReader, procesamiento, reintentos);
        this.mailSender = mailSender;
        this.templates = templates;
        this.fromAddress = fromAddress;
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email}", ackMode = "MANUAL")
    public void processEmail(Message message, Channel channel) {
        resolver(message, channel);
    }

    @Override
    protected void procesar(Envelope envelope, String traceId) {
        String destinatario = direccionValida(envelope);
        EmailTemplates.Correo correo = construir(envelope);

        log.info("[Email] Enviando a={} asunto='{}' traceId={}",
                destinatario, correo.asunto(), traceId);

        SimpleMailMessage mail = new SimpleMailMessage();
        mail.setFrom(fromAddress);
        mail.setTo(destinatario);
        mail.setSubject(correo.asunto());
        mail.setText(correo.cuerpo());

        // Cualquier MailException sale de aquí sin tocarse: es el caso transitorio, y el
        // catch(Exception) de la clase base lo clasifica en el camino 4 o en el 5 según el intento.
        mailSender.send(mail);

        log.info("[Email] Enviado correctamente traceId={}", traceId);
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
