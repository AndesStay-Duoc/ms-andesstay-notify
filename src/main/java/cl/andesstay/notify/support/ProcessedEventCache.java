package cl.andesstay.notify.support;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registro de los eventId ya procesados, para descartar duplicados en silencio.
 *
 * Es obligatorio porque RabbitMQ garantiza entrega "al menos una vez": un ack perdido reentrega un
 * mensaje ya procesado. Sin esta comprobación, un huésped recibiría el mismo correo dos veces. Ver
 * infra/docs/contracts/events/envelope.md, sección de idempotencia.
 *
 * ms-andesstay-notify no tiene base de datos, así que el registro es en memoria con expiración, que
 * es justo lo que el contrato autoriza para los consumidores sin persistencia. La consecuencia hay
 * que tenerla presente: al reiniciar el servicio el registro se pierde, de modo que un mensaje
 * reentregado justo después de un reinicio sí se podría reprocesar. La ventana es de segundos y el
 * efecto es un correo duplicado, no una inconsistencia de datos.
 */
@Slf4j
@Component
public class ProcessedEventCache {

    private final Map<String, Long> procesados = new ConcurrentHashMap<>();
    private final long ttlMs;
    private final int capacidadMaxima;

    public ProcessedEventCache(
            @Value("${andesstay.notify.idempotencia.ttl:PT6H}") Duration ttl,
            @Value("${andesstay.notify.idempotencia.capacidad-maxima:50000}") int capacidadMaxima) {
        this.ttlMs = ttl.toMillis();
        this.capacidadMaxima = capacidadMaxima;
    }

    /**
     * Registra el eventId y responde si es la primera vez que se ve.
     *
     * La operación es atómica: {@code putIfAbsent} devuelve null solo para el primer llamador, así
     * que dos consumidores concurrentes sobre el mismo eventId no pueden ejecutar los dos el
     * efecto.
     */
    public boolean esNuevo(String eventId) {
        long ahora = System.currentTimeMillis();
        Long visto = procesados.putIfAbsent(eventId, ahora);

        if (visto == null) {
            if (procesados.size() > capacidadMaxima) {
                purgar(ahora);
            }
            return true;
        }

        // Un registro vencido se trata como nuevo: pasado el TTL se asume que ninguna reentrega
        // legítima puede seguir en vuelo.
        if (ahora - visto > ttlMs) {
            procesados.put(eventId, ahora);
            return true;
        }
        return false;
    }

    /**
     * Libera la reserva de un eventId cuyo procesamiento falló.
     *
     * Es imprescindible, y no una comodidad. {@link #esNuevo} reserva ANTES de ejecutar el efecto,
     * porque reservar después dejaría una ventana en la que dos consumidores concurrentes podrían
     * ejecutarlo los dos. Pero si el trabajo falla y la reserva se queda puesta, pasan dos cosas
     * malas: el reintento ve su propio primer intento como duplicado, retorna sin error y el
     * mensaje se confirma como si hubiera funcionado —nunca llega a la DLQ—, y además el eventId
     * queda marcado como procesado cuando en realidad no lo fue.
     */
    public void liberar(String eventId) {
        procesados.remove(eventId);
    }

    /** Solo para los tests y para el endpoint de estado. */
    public int tamano() {
        return procesados.size();
    }

    private void purgar(long ahora) {
        int antes = procesados.size();
        Iterator<Map.Entry<String, Long>> it = procesados.entrySet().iterator();
        while (it.hasNext()) {
            if (ahora - it.next().getValue() > ttlMs) {
                it.remove();
            }
        }
        // Si la purga por TTL no bastó, el registro se vacía por completo. Perder la protección es
        // preferible a que el servicio consuma memoria sin límite, y con 50 000 entradas vivas en
        // menos de seis horas el problema real está aguas arriba.
        if (procesados.size() > capacidadMaxima) {
            log.warn("[Idempotencia] {} eventId vigentes supera la capacidad de {}; se vacía el registro",
                    procesados.size(), capacidadMaxima);
            procesados.clear();
        } else if (antes != procesados.size()) {
            log.debug("[Idempotencia] purgados {} eventId vencidos", antes - procesados.size());
        }
    }
}
