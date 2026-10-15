package cl.andesstay.notify.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * Traduce el rechazo de @PreAuthorize al código del contrato, igual que en audit y report.
 *
 * Es el único caso que este servicio necesita: su superficie HTTP es un solo endpoint de lectura.
 * Sin este manejador, un rol insuficiente salía como 500 con una traza de Spring.
 *
 * Deliberadamente NO se captura Exception a secas: un fallo inesperado tiene que seguir siendo un
 * 500 con su traza en el log, o se esconderían defectos reales.
 */
@RestControllerAdvice
public class ManejadorDeErrores {

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Map<String, Object>> sinPermiso(AccessDeniedException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("error", "ROL_INSUFICIENTE"));
    }
}
