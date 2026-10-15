package cl.andesstay.notify.support;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Métricas del flujo de notificaciones, expuestas por Actuator.
 *
 * Las cuatro que promete infra/docs/contracts/events/rabbit.md:
 *
 *   notify.messages.processed   mensajes procesados con éxito, por cola
 *   notify.messages.retried     reintentos, por cola
 *   notify.messages.dlq         mensajes que llegaron a una DLQ, por cola
 *   notify.dlq.rate             proporción de mensajes que terminan en DLQ
 *
 * La tasa de DLQ es el indicador de salud del sistema: si sube, algo cambió aguas arriba.
 */
@Slf4j
@Component
public class NotifyMetrics {

    private final MeterRegistry registry;

    private final Map<String, Counter> procesados = new ConcurrentHashMap<>();
    private final Map<String, Counter> reintentos = new ConcurrentHashMap<>();
    private final Map<String, Counter> aLaDlq = new ConcurrentHashMap<>();

    // Totales propios, porque el gauge necesita leer el acumulado y un Counter de Micrometer no
    // garantiza lectura exacta en todos los backends.
    private final AtomicLong totalProcesados = new AtomicLong();
    private final AtomicLong totalALaDlq = new AtomicLong();

    public NotifyMetrics(MeterRegistry registry) {
        this.registry = registry;

        Gauge.builder("notify.dlq.rate", this, NotifyMetrics::tasaDlq)
                .description("Proporción de mensajes que terminan en una DLQ")
                .register(registry);
    }

    public void procesado(String cola) {
        procesados.computeIfAbsent(cola, c -> Counter
                        .builder("notify.messages.processed")
                        .description("Mensajes procesados con éxito")
                        .tag("queue", c)
                        .register(registry))
                .increment();
        totalProcesados.incrementAndGet();
    }

    public void enviadoALaDlq(String cola) {
        aLaDlq.computeIfAbsent(cola, c -> Counter
                        .builder("notify.messages.dlq")
                        .description("Mensajes que llegaron a una DLQ")
                        .tag("queue", c)
                        .register(registry))
                .increment();
        totalALaDlq.incrementAndGet();
    }

    /**
     * Cuenta la invocación como reintento cuando no es el primer intento del mensaje.
     *
     * EL INTENTO LLEGA COMO PARÁMETRO, y no es cosmética. Antes se leía del RetryContext que
     * Spring Retry pone en el hilo, y ese mecanismo ya no existe: el reintento lo hace el broker
     * con la cola de espera, no un interceptor en proceso.
     * RetrySynchronizationManager.getContext() devolvería siempre null, la condición nunca se
     * cumpliría y notify.messages.retried se quedaría clavado en CERO para siempre, sin que
     * nada más fallara. Es la peor clase de defecto: el servicio funciona, el log dice que hubo
     * reintentos y la métrica que los cuenta miente.
     *
     * El valor viene de la cabecera x-andesstay-intentos, que escribe ReintentoPublisher. No se
     * puede sacar de messageProperties.isRedelivered(): esa bandera indica una reentrega del
     * broker sobre la MISMA cola, y aquí el mensaje llega como una entrega nueva desde la cola
     * de espera, con redelivered en false.
     */
    public void registrarIntento(String cola, int intento) {
        if (intento <= 1) {
            return;
        }
        reintentos.computeIfAbsent(cola, c -> Counter
                        .builder("notify.messages.retried")
                        .description("Reintentos de procesamiento")
                        .tag("queue", c)
                        .register(registry))
                .increment();
        log.debug("[Métricas] cola={} intento={}", cola, intento);
    }

    public double tasaDlq() {
        long exitos = totalProcesados.get();
        long muertos = totalALaDlq.get();
        long total = exitos + muertos;
        return total == 0 ? 0d : (double) muertos / total;
    }
}
