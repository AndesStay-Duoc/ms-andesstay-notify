package cl.andesstay.notify.support;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetrySynchronizationManager;
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
     * Cuenta la invocación como reintento si el interceptor de Spring Retry ya la había intentado
     * antes.
     *
     * El reintento de Spring es en proceso y sin reentrega del broker, así que no se puede detectar
     * con {@code messageProperties.isRedelivered()}: la entrega es la misma. El contexto de retry,
     * en cambio, está disponible en el hilo mientras el interceptor está activo y lleva la cuenta
     * de intentos.
     */
    public void registrarIntento(String cola) {
        RetryContext contexto = RetrySynchronizationManager.getContext();
        if (contexto != null && contexto.getRetryCount() > 0) {
            reintentos.computeIfAbsent(cola, c -> Counter
                            .builder("notify.messages.retried")
                            .description("Reintentos de procesamiento")
                            .tag("queue", c)
                            .register(registry))
                    .increment();
            log.debug("[Métricas] cola={} intento={}", cola, contexto.getRetryCount() + 1);
        }
    }

    public double tasaDlq() {
        long exitos = totalProcesados.get();
        long muertos = totalALaDlq.get();
        long total = exitos + muertos;
        return total == 0 ? 0d : (double) muertos / total;
    }
}
