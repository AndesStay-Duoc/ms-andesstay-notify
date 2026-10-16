package cl.andesstay.notify.service;

import cl.andesstay.notify.email.EmailTemplates;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Envía el correo al huésped. No sabe nada de RabbitMQ: recibe una {@link Solicitud} ya armada.
 *
 * Los dos tipos de error que puede devolver:
 *   permanente  (destinatario o plantilla malos) -> SolicitudInvalidaException
 *   transitorio (servidor SMTP caído, timeout)  -> la MailException de Spring, sin tocar
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CorreoService {

    private final JavaMailSender mailSender;
    private final EmailTemplates templates;

    @Value("${andesstay.mail.from}")
    private String fromAddress;

    /**
     * Lo que hace falta para enviar un correo.
     *
     * El formato contratado trae {@code plantilla} y {@code datos}; el anterior traía
     * {@code asunto} y {@code cuerpo} ya armados. Se aceptan los dos porque la publicación migra
     * al formato nuevo en un despliegue posterior al de este servicio.
     */
    public record Solicitud(String destinatario, String plantilla, String reservationId,
                            Map<String, Object> datos, String asunto, String cuerpo) {
    }

    public void enviar(Solicitud solicitud, String traceId) {
        String destinatario = direccionValida(solicitud.destinatario());
        EmailTemplates.Correo correo = construir(solicitud);

        log.info("[Email] Enviando a={} asunto='{}' traceId={}",
                destinatario, correo.asunto(), traceId);

        SimpleMailMessage mail = new SimpleMailMessage();
        mail.setFrom(fromAddress);
        mail.setTo(destinatario);
        mail.setSubject(correo.asunto());
        mail.setText(correo.cuerpo());

        // Cualquier MailException sale de aquí sin tocarse: es el caso transitorio y lo tiene que
        // ver el interceptor de reintentos.
        mailSender.send(mail);

        log.info("[Email] Enviado correctamente traceId={}", traceId);
    }

    private EmailTemplates.Correo construir(Solicitud solicitud) {
        if (solicitud.plantilla() != null) {
            return templates.render(solicitud.plantilla(), solicitud.reservationId(), solicitud.datos());
        }
        if (solicitud.asunto() == null || solicitud.cuerpo() == null) {
            throw new SolicitudInvalidaException("la solicitud no trae ni plantilla ni asunto/cuerpo");
        }
        return new EmailTemplates.Correo(solicitud.asunto(), solicitud.cuerpo());
    }

    /**
     * Un destinatario ausente o mal formado no se arregla reintentando, así que se rechaza antes de
     * molestar al servidor de correo.
     */
    private static String direccionValida(String destinatario) {
        if (destinatario == null) {
            throw new SolicitudInvalidaException("el payload no trae destinatario");
        }
        try {
            new InternetAddress(destinatario).validate();
        } catch (AddressException e) {
            throw new SolicitudInvalidaException(
                    "destinatario inválido '" + destinatario + "': " + e.getMessage());
        }
        return destinatario;
    }
}
