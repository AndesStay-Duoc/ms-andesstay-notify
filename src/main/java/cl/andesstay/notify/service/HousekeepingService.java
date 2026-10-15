package cl.andesstay.notify.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Emite el ticket de preparación o de limpieza de la unidad. Es lo que resuelve el problema de que
 * housekeeping se enteraba tarde. No sabe nada de RabbitMQ.
 */
@Slf4j
@Service
public class HousekeepingService {

    /** PREPARACION al pasar a CHECKIN_PENDIENTE, LIMPIEZA al pasar a CHECKOUT. */
    private static final Set<String> TAREAS = Set.of("PREPARACION", "LIMPIEZA", "PREPARE", "CLEAN");

    public record Ticket(String tipoTarea, String fechaRequerida, Object unitId, Object reservationId,
                         String unidad, String prioridad, String notas) {
    }

    public void emitir(Ticket ticket, String traceId) {
        if (ticket.tipoTarea() == null || !TAREAS.contains(ticket.tipoTarea())) {
            throw new SolicitudInvalidaException(
                    "tipoTarea desconocido: " + ticket.tipoTarea() + " (conocidos: " + TAREAS + ")");
        }
        if (ticket.unitId() == null) {
            throw new SolicitudInvalidaException("el payload no trae unitId");
        }

        // TODO: integrar con el sistema de tickets del hostal o enviar push al equipo. Hasta
        // entonces el ticket queda en el log con todos sus datos, que es lo que consume la
        // evidencia de la fase 6.
        log.info("[Housekeeping] TICKET {} → unitId={} unidad={} reserva={} fecha={} prioridad={} notas={} traceId={}",
                ticket.tipoTarea(), ticket.unitId(), ticket.unidad(), ticket.reservationId(),
                ticket.fechaRequerida(), ticket.prioridad(), ticket.notas(), traceId);
    }
}
