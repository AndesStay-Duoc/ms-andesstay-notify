package cl.andesstay.notify.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;

/**
 * Sobre común de todo lo que viaja por RabbitMQ y por Kafka.
 * Ver infra/docs/contracts/events/envelope.md.
 *
 * Se ignoran los campos desconocidos a propósito: el contrato establece que agregar un campo
 * opcional al payload es compatible y que un consumidor nunca falla por encontrar campos que no
 * esperaba.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Envelope(
        String type,
        String eventId,
        String timestamp,
        String traceId,
        String correlationId,
        String source,
        Actor actor,
        Map<String, Object> payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Actor(String userId, String role, String tenant) {
    }

    /** Para los logs, cuando el traceId puede venir ausente. */
    public String traceIdODesconocido() {
        return traceId == null || traceId.isBlank() ? "(sin traceId)" : traceId;
    }

    /**
     * Lee una cadena del payload probando varias claves, en orden de preferencia.
     *
     * Existe porque la publicación migra al formato contratado en un despliegue posterior al de
     * este servicio: durante esa ventana conviven el payload nuevo ({@code destinatario}) y el
     * antiguo ({@code to}). Sin esto, los dos repositorios tendrían que desplegarse a la vez.
     */
    public String texto(String... claves) {
        if (payload == null) {
            return null;
        }
        for (String clave : claves) {
            Object valor = payload.get(clave);
            if (valor instanceof String s && !s.isBlank()) {
                return s;
            }
        }
        return null;
    }

    public Object valor(String clave) {
        return payload == null ? null : payload.get(clave);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> datos() {
        Object datos = valor("datos");
        return datos instanceof Map<?, ?> mapa ? (Map<String, Object>) mapa : Map.of();
    }
}
