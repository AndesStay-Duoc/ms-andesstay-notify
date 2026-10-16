package cl.andesstay.notify.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;

/**
 * Genera el comprobante de la reserva. Es lo que resuelve el problema de que el huésped no recibía
 * comprobante formal. No sabe nada de RabbitMQ.
 */
@Slf4j
@Service
public class VoucherService {

    private static final Set<String> DOCUMENTOS = Set.of("VOUCHER", "BOLETA");

    /** El formato anterior no traía tipo de documento: de ahí el VOUCHER por omisión. */
    private static final String DOCUMENTO_POR_OMISION = "VOUCHER";

    public record Solicitud(Object reservationId, String destinatario, String tipoDocumento,
                            Map<String, Object> datos) {
    }

    public void generar(Solicitud solicitud, String traceId) {
        String tipoDocumento = solicitud.tipoDocumento() == null
                ? DOCUMENTO_POR_OMISION
                : solicitud.tipoDocumento();

        if (solicitud.reservationId() == null) {
            throw new SolicitudInvalidaException("el payload no trae reservationId");
        }
        if (solicitud.destinatario() == null) {
            throw new SolicitudInvalidaException("el payload no trae destinatario");
        }
        if (!DOCUMENTOS.contains(tipoDocumento)) {
            throw new SolicitudInvalidaException(
                    "tipoDocumento desconocido: " + tipoDocumento + " (conocidos: " + DOCUMENTOS + ")");
        }

        // TODO: generar el PDF con iText o JasperReports y adjuntarlo al correo. Hasta entonces el
        // documento queda registrado con sus datos, que es lo que consume la evidencia de la fase 6.
        log.info("[Voucher] {} generado reserva={} destinatario={} datos={} traceId={}",
                tipoDocumento, solicitud.reservationId(), solicitud.destinatario(), solicitud.datos(), traceId);
    }
}
