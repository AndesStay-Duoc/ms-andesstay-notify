package cl.andesstay.notify.controller;

import cl.andesstay.notify.service.EstadoDeColasService;
import cl.andesstay.notify.support.NotifyMetrics;
import cl.andesstay.notify.support.ProcessedEventCache;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Estado del flujo de colas: profundidad de las nueve colas, tasa de DLQ y tamaño del registro de
 * idempotencia. Es el equivalente del endpoint de estado que expone la guía 2.2.3.
 *
 * El controlador no habla con el broker. La lectura de las colas vive en EstadoDeColasService: antes
 * estaba aquí, con AmqpAdmin inyectado, y la capa web conocía la infraestructura de mensajería. La
 * respuesta es la misma, con las mismas claves y en el mismo orden. NotifyMetrics y
 * ProcessedEventCache se quedan porque son contadores de la propia aplicación, no del broker.
 *
 * Exige un JWT de Azure AD con el rol Admin, como los endpoints de reportería (ver SecurityConfig).
 *
 * Además, deliberadamente NO se publica en el API Gateway y el puerto 8085 no se abre en el
 * Security Group de ec2-mq: sirve para la evidencia y para depurar por túnel SSH, no para la
 * aplicación.
 */
@RestController
@RequestMapping("/api/v1/notify")
@RequiredArgsConstructor
public class NotifyStatusController {

    private final EstadoDeColasService colas;
    private final NotifyMetrics metrics;
    private final ProcessedEventCache procesados;

    @GetMapping("/status")
    @PreAuthorize("hasRole('Admin')")
    public Map<String, Object> estado() {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("servicio", "ms-andesstay-notify");
        respuesta.put("timestamp", Instant.now().toString());
        respuesta.put("flujos", colas.flujos());
        respuesta.put("tasaDlq", metrics.tasaDlq());
        respuesta.put("eventIdEnRegistro", procesados.tamano());
        return respuesta;
    }
}
