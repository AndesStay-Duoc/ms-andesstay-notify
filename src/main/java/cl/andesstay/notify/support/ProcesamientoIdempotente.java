package cl.andesstay.notify.support;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Envuelve el procesamiento de un mensaje con las tres cosas que los tres consumidores necesitan
 * hacer igual: contar el intento, descartar duplicados y contar el éxito.
 *
 * Estaba repetido en cada consumidor y la secuencia es delicada, así que vive en un solo sitio.
 *
 * Con ACK manual el consumidor necesita saber CUÁL de los dos caminos de éxito ocurrió, porque los
 * dos terminan en un basicAck pero tienen que quedar distinguidos en el log y en las métricas. De
 * ahí el Resultado: antes bastaba un void, porque del ack se encargaba el contenedor.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ProcesamientoIdempotente {

    /** Los dos desenlaces que acaban confirmando el mensaje. */
    public enum Resultado {
        /** Se ejecutó el efecto. */
        PROCESADO,
        /** Ya se había ejecutado en una entrega anterior cuyo ack se perdió. */
        DUPLICADO
    }

    private final ProcessedEventCache cache;
    private final NotifyMetrics metrics;

    /**
     * Ejecuta el trabajo una sola vez por eventId.
     *
     * EL BLOQUE CATCH NO SE TRAGA LA EXCEPCIÓN: libera la reserva de idempotencia y la vuelve a
     * lanzar. Con ACK manual eso sigue siendo imprescindible, y por el mismo motivo que antes
     * aunque el mecanismo de reintento haya cambiado.
     *
     * El mensaje que llega desde la cola de espera trae el MISMO eventId: es una copia del
     * original, no un evento nuevo. Si la reserva se quedara puesta, ese reintento vería su propio
     * intento anterior como duplicado, el consumidor tomaría el camino del duplicado y haría
     * basicAck. El resultado sería que el correo nunca se envía, el mensaje se confirma como
     * procesado, no queda nada en ninguna DLQ y notify.dlq.rate sigue en cero: un fallo silencioso
     * con métricas en verde, que es la peor categoría de fallo.
     *
     * Liberar abre una ventana en la que el eventId no está reservado, así que una entrega
     * concurrente del mismo evento podría pasar. Hacen falta dos cosas a la vez —dos copias del
     * mismo eventId en vuelo y dos consumidores atendiéndolas— y con prefetch 1 y un consumidor
     * por cola es inalcanzable. El canje es claro: la ventana cuesta, como máximo, un correo
     * duplicado; no liberar cuesta que el reintento y la DLQ dejen de funcionar.
     *
     * El intento llega como parámetro. Antes se leía del RetryContext de Spring Retry, que ya no
     * existe: el reintento ahora pasa por el broker y no por un interceptor en proceso.
     */
    public Resultado ejecutar(String cola, String eventId, String traceId, int intento,
                              Runnable trabajo) {
        metrics.registrarIntento(cola, intento);

        if (!cache.esNuevo(eventId)) {
            log.info("[{}] Duplicado descartado en silencio eventId={} traceId={}",
                    cola, eventId, traceId);
            return Resultado.DUPLICADO;
        }

        try {
            trabajo.run();
        } catch (RuntimeException e) {
            cache.liberar(eventId);
            throw e;
        }

        metrics.procesado(cola);
        return Resultado.PROCESADO;
    }
}
