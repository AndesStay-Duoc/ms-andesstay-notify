package cl.andesstay.notify.controller;

import cl.andesstay.notify.support.NotifyMetrics;
import cl.andesstay.notify.support.ProcessedEventCache;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Estado del flujo de colas: profundidad de las nueve colas, tasa de DLQ y tamaño del registro de
 * idempotencia. Es el equivalente del endpoint de estado que expone la guía 2.2.3.
 *
 * Las colas se agrupan por flujo y no en una lista plana, porque el conteo importa: tres flujos con
 * tres colas cada uno se lee como un patrón repetido, mientras que nueve nombres sueltos invitan a
 * preguntar por qué no son seis.
 *
 * La profundidad de las colas de espera es una medida que el rediseño hizo posible: mensajes con
 * reintento pendiente AHORA MISMO. Con el reintento en proceso de Spring Retry no existía, porque
 * la espera vivía en un hilo dormido y no en el broker.
 *
 * Deliberadamente NO se publica en el API Gateway. ms-andesstay-notify no tiene starter de
 * security, así que su superficie HTTP solo debe ser alcanzable desde dentro de la instancia: el
 * puerto 8085 no se abre en el Security Group de ec2-mq. Sirve para la evidencia y para depurar
 * por túnel SSH, no para la aplicación.
 */
@RestController
@RequestMapping("/api/v1/notify")
@RequiredArgsConstructor
public class NotifyStatusController {

    /**
     * Se inyecta la INTERFAZ, no RabbitAdmin.
     *
     * Spring Boot registra el bean con el tipo declarado AmqpAdmin, así que pedir un RabbitAdmin
     * falla en el arranque con "No qualifying bean of type RabbitAdmin available", aunque la
     * instancia concreta sea exactamente esa. getQueueProperties pertenece a la interfaz y
     * devuelve el conteo de mensajes y de consumidores, que es todo lo que hace falta aquí.
     */
    private final AmqpAdmin amqpAdmin;
    private final NotifyMetrics metrics;
    private final ProcessedEventCache procesados;

    @Value("${andesstay.rabbitmq.queues.email}")
    private String colaEmail;
    @Value("${andesstay.rabbitmq.queues.email-retry}")
    private String esperaEmail;
    @Value("${andesstay.rabbitmq.queues.email-dlq}")
    private String dlqEmail;

    @Value("${andesstay.rabbitmq.queues.housekeeping}")
    private String colaHousekeeping;
    @Value("${andesstay.rabbitmq.queues.housekeeping-retry}")
    private String esperaHousekeeping;
    @Value("${andesstay.rabbitmq.queues.housekeeping-dlq}")
    private String dlqHousekeeping;

    @Value("${andesstay.rabbitmq.queues.voucher}")
    private String colaVoucher;
    @Value("${andesstay.rabbitmq.queues.voucher-retry}")
    private String esperaVoucher;
    @Value("${andesstay.rabbitmq.queues.voucher-dlq}")
    private String dlqVoucher;

    @GetMapping("/status")
    public Map<String, Object> estado() {
        Map<String, Object> flujos = new LinkedHashMap<>();
        flujos.put("email", describirFlujo(colaEmail, esperaEmail, dlqEmail));
        flujos.put("housekeeping", describirFlujo(colaHousekeeping, esperaHousekeeping, dlqHousekeeping));
        flujos.put("voucher", describirFlujo(colaVoucher, esperaVoucher, dlqVoucher));

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("servicio", "ms-andesstay-notify");
        respuesta.put("timestamp", Instant.now().toString());
        respuesta.put("flujos", flujos);
        respuesta.put("tasaDlq", metrics.tasaDlq());
        respuesta.put("eventIdEnRegistro", procesados.tamano());
        return respuesta;
    }

    /**
     * Las tres colas de un flujo, con el papel de cada una explícito en la clave.
     *
     * "espera" y no "retry" porque es lo que la cola hace: el mensaje aguarda ahí su siguiente
     * intento y nadie la consume. Un valor distinto de cero significa reintentos en curso en este
     * instante.
     */
    private Map<String, Object> describirFlujo(String trabajo, String espera, String dlq) {
        Map<String, Object> flujo = new LinkedHashMap<>();
        flujo.put("trabajo", Map.of(trabajo, describir(trabajo)));
        flujo.put("espera", Map.of(espera, describir(espera)));
        flujo.put("cuarentena", Map.of(dlq, describir(dlq)));
        return flujo;
    }

    private Object describir(String nombre) {
        try {
            Properties info = amqpAdmin.getQueueProperties(nombre);
            if (info == null) {
                // La cola no existe en el broker. Con la topología declarada en código no debería
                // pasar, así que vale la pena que se vea en la respuesta en lugar de un 0 engañoso.
                return "no existe en el broker";
            }
            return Map.of(
                    "mensajes", info.get(RabbitAdmin.QUEUE_MESSAGE_COUNT),
                    "consumidores", info.get(RabbitAdmin.QUEUE_CONSUMER_COUNT));
        } catch (Exception e) {
            return "no consultable: " + e.getMessage();
        }
    }
}
