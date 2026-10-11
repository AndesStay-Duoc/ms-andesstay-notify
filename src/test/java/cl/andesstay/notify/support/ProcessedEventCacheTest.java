package cl.andesstay.notify.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * El contrato de idempotencia (infra/docs/contracts/events/envelope.md) exige que un eventId
 * repetido se descarte en silencio. RabbitMQ entrega al menos una vez, así que esto es lo que
 * impide que el huésped reciba el mismo correo dos veces.
 */
class ProcessedEventCacheTest {

    @Test
    @DisplayName("el primer eventId es nuevo y el segundo no")
    void descartaElDuplicado() {
        ProcessedEventCache cache = new ProcessedEventCache(Duration.ofHours(6), 1000);
        String eventId = UUID.randomUUID().toString();

        assertTrue(cache.esNuevo(eventId), "la primera vez tiene que procesarse");
        assertFalse(cache.esNuevo(eventId), "la segunda vez tiene que descartarse");
        assertFalse(cache.esNuevo(eventId));
    }

    @Test
    @DisplayName("eventId distintos no se interfieren")
    void distinguePorEventId() {
        ProcessedEventCache cache = new ProcessedEventCache(Duration.ofHours(6), 1000);

        assertTrue(cache.esNuevo("a"));
        assertTrue(cache.esNuevo("b"));
        assertFalse(cache.esNuevo("a"));
        assertEquals(2, cache.tamano());
    }

    @Test
    @DisplayName("pasado el TTL el eventId se vuelve a aceptar")
    void venceConElTtl() throws InterruptedException {
        ProcessedEventCache cache = new ProcessedEventCache(Duration.ofMillis(30), 1000);

        assertTrue(cache.esNuevo("x"));
        assertFalse(cache.esNuevo("x"));

        Thread.sleep(60);

        assertTrue(cache.esNuevo("x"), "vencido el TTL, ninguna reentrega legítima sigue en vuelo");
    }

    @Test
    @DisplayName("con varios consumidores concurrentes sobre el mismo eventId solo uno gana")
    void soloUnGanadorEnConcurrencia() throws InterruptedException {
        // Es la razón por la que el registro usa putIfAbsent y no un get seguido de un put: el
        // prefetch puede ser 1, pero nada impide que mañana haya varios consumidores por cola.
        ProcessedEventCache cache = new ProcessedEventCache(Duration.ofHours(6), 1000);
        int hilos = 16;
        AtomicInteger ganadores = new AtomicInteger();
        CountDownLatch salida = new CountDownLatch(1);
        CountDownLatch fin = new CountDownLatch(hilos);

        ExecutorService pool = Executors.newFixedThreadPool(hilos);
        try {
            for (int i = 0; i < hilos; i++) {
                pool.submit(() -> {
                    try {
                        salida.await();
                        if (cache.esNuevo("mismo-evento")) {
                            ganadores.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        fin.countDown();
                    }
                });
            }
            salida.countDown();
            assertTrue(fin.await(5, TimeUnit.SECONDS), "los hilos tenían que terminar");
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, ganadores.get(), "el efecto solo puede ejecutarse una vez");
    }
}
