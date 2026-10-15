package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.CorreoService;
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
 * El envío lo hace {@link CorreoService}; este consumidor solo traduce el mensaje a una solicitud.
 * Reparto de responsabilidades entre los dos tipos de error:
 *
 *   transitorio (servidor SMTP caído, timeout)   -> se propaga tal cual, se reintenta
 *   permanente  (destinatario o plantilla malos) -> SolicitudInvalidaException en el servicio, que
 *                                                   permanenteComoRechazo convierte en
 *                                                   AmqpRejectAndDontRequeueException: a la DLQ ya
 */
@Slf4j
@Component
public class EmailConsumer extends ConsumidorDeTrabajo {

    private final CorreoService correoService;

    public EmailConsumer(
            @Value("${andesstay.rabbitmq.queues.email}") String cola,
            @Value("${andesstay.rabbitmq.queues.email-retry}") String colaDeEspera,
            CorreoService correoService,
            EnvelopeReader envelopeReader,
            ProcesamientoIdempotente procesamiento,
            ReintentoPublisher reintentos) {
        // El nombre de la cola sale de la MISMA propiedad que el @RabbitListener de abajo, en
        // lugar de una constante aparte: así la etiqueta de las métricas no puede desincronizarse
        // de la cola que realmente se escucha.
        super(cola, colaDeEspera, "Email", envelopeReader, procesamiento, reintentos);
        this.correoService = correoService;
    }

    @RabbitListener(queues = "${andesstay.rabbitmq.queues.email}", ackMode = "MANUAL")
    public void processEmail(Message message, Channel channel) {
        resolver(message, channel);
    }

    /**
     * Cualquier MailException sale del servicio sin tocarse: es el caso transitorio, y el
     * catch(Exception) de la clase base lo clasifica en el camino 4 o en el 5 según el intento.
     * Solo SolicitudInvalidaException se convierte en rechazo permanente (camino 3).
     */
    @Override
    protected void procesar(Envelope envelope, String traceId) {
        permanenteComoRechazo(() -> correoService.enviar(aSolicitud(envelope), traceId)).run();
    }

    /**
     * Traduce el mensaje a la solicitud del servicio.
     *
     * El formato contratado trae {@code plantilla} y {@code datos}; el anterior traía
     * {@code subject} y {@code body} ya armados. Se pasan los dos porque la publicación migra al
     * formato nuevo en un despliegue posterior al de este servicio.
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
