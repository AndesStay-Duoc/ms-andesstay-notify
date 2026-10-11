package cl.andesstay.notify.email;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * Plantillas de correo al huésped. Las cuatro que fija
 * infra/docs/contracts/events/rabbit.md.
 *
 * Una plantilla inexistente es un error PERMANENTE: no se arregla reintentando, así que el mensaje
 * va directo a la DLQ. Es además el caso de prueba más limpio para demostrar que la distinción
 * entre error transitorio y permanente funciona.
 */
@Component
public class EmailTemplates {

    public static final Set<String> CONOCIDAS = Set.of(
            "RESERVA_RECIBIDA",
            "RESERVA_CONFIRMADA",
            "RECORDATORIO_CHECKIN",
            "CHECKOUT_REALIZADO",
            "RESERVA_CANCELADA");

    public record Correo(String asunto, String cuerpo) {
    }

    public Correo render(String plantilla, String reservationId, Map<String, Object> datos) {
        if (plantilla == null || !CONOCIDAS.contains(plantilla)) {
            throw new AmqpRejectAndDontRequeueException(
                    "plantilla desconocida: " + plantilla + " (conocidas: " + CONOCIDAS + ")");
        }

        String huesped = texto(datos, "nombreHuesped", "huésped");
        String unidad = texto(datos, "unidad", "la unidad reservada");
        String entrada = texto(datos, "fechaEntrada", "(sin fecha)");
        String salida = texto(datos, "fechaSalida", "(sin fecha)");
        String total = texto(datos, "tarifaTotal", null);
        String referencia = reservationId == null ? "" : " #" + reservationId;

        return switch (plantilla) {
            // Acuse de recibo. No está en la tabla de rabbit.md, que empieza en la confirmación,
            // pero el huésped necesita saber que su solicitud llegó antes de que recepción la
            // confirme.
            case "RESERVA_RECIBIDA" -> new Correo(
                    "Reserva recibida - AndesStay" + referencia,
                    """
                    Hola %s:

                    La solicitud de reserva%s quedó registrada.

                      Unidad:  %s
                      Entrada: %s
                      Salida:  %s

                    Recepción la confirmará en breve y recibirás el voucher.
                    """.formatted(huesped, referencia, unidad, entrada, salida));

            case "RESERVA_CONFIRMADA" -> new Correo(
                    "Reserva confirmada - AndesStay" + referencia,
                    """
                    Hola %s:

                    La reserva%s quedó confirmada.

                      Unidad:  %s
                      Entrada: %s
                      Salida:  %s%s

                    El equipo de AndesStay espera tu llegada.
                    """.formatted(huesped, referencia, unidad, entrada, salida, lineaTotal(total)));

            case "RECORDATORIO_CHECKIN" -> new Correo(
                    "Tu check-in es pronto - AndesStay" + referencia,
                    """
                    Hola %s:

                    El check-in de la reserva%s es el %s en %s.

                    La unidad ya está en preparación.
                    """.formatted(huesped, referencia, entrada, unidad));

            case "CHECKOUT_REALIZADO" -> new Correo(
                    "Gracias por tu estadía - AndesStay" + referencia,
                    """
                    Hola %s:

                    El check-out de %s se registró el %s.

                    Gracias por elegir AndesStay.
                    """.formatted(huesped, unidad, salida));

            case "RESERVA_CANCELADA" -> new Correo(
                    "Reserva cancelada - AndesStay" + referencia,
                    """
                    Hola %s:

                    La reserva%s para %s, del %s al %s, quedó cancelada.

                    Si no fue una solicitud tuya, responde a este correo.
                    """.formatted(huesped, referencia, unidad, entrada, salida));

            // El switch es exhaustivo sobre CONOCIDAS y la validación de arriba ya filtró el resto;
            // esta rama solo existe para que agregar una plantilla al Set sin agregar su cuerpo
            // falle de forma explícita en vez de enviar un correo vacío.
            default -> throw new AmqpRejectAndDontRequeueException(
                    "plantilla declarada pero sin cuerpo: " + plantilla);
        };
    }

    private String lineaTotal(String total) {
        return total == null ? "" : "%n  Total:   $%s CLP".formatted(total);
    }

    private String texto(Map<String, Object> datos, String clave, String porOmision) {
        Object valor = datos == null ? null : datos.get(clave);
        if (valor == null || valor.toString().isBlank()) {
            return porOmision;
        }
        return valor.toString();
    }
}
