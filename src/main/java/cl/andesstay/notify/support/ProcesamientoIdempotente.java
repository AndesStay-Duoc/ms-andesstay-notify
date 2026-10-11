package cl.andesstay.notify.support;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Envuelve el procesamiento de un mensaje con las tres cosas que los tres consumidores necesitan
 * hacer igual: contar el intento, descartar duplicados y contar el éxito.
 *
 * Estaba repetido en cada consumidor y la secuencia es delicada, así que vive en un solo sitio.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcesamientoIdempotente {

    private final ProcessedEventCache cache;
    private final NotifyMetrics metrics;

    /**
     * Ejecuta el trabajo una sola vez por eventId.
     *
     * El bloque catch NO se traga la excepción: libera la reserva de idempotencia y la vuelve a
     * lanzar. Eso es lo que permite que el interceptor de reintentos de Spring la vea y, agotados
     * los intentos, que el contenedor rechace el mensaje y el broker lo mande a su DLQ.
     *
     * Sin el liberar, el segundo intento encontraría su propia reserva del primero, la trataría
     * como duplicado y confirmaría el mensaje como procesado: el fallo desaparecería en silencio
     * en lugar de quedar en cuarentena.
     */
    public void ejecutar(String cola, String eventId, String traceId, Runnable trabajo) {
        metrics.registrarIntento(cola);

        if (!cache.esNuevo(eventId)) {
            log.info("[{}] Duplicado descartado en silencio eventId={} traceId={}", cola, eventId, traceId);
            return;
        }

        try {
            trabajo.run();
        } catch (RuntimeException e) {
            cache.liberar(eventId);
            throw e;
        }

        metrics.procesado(cola);
    }
}
