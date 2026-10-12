package cl.andesstay.notify.controller;

import cl.andesstay.notify.support.NotifyMetrics;
import cl.andesstay.notify.support.ProcessedEventCache;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Estado del flujo de colas: profundidad de las seis colas, tasa de DLQ y tamaño del registro de
 * idempotencia. Es el equivalente del endpoint de estado que expone la guía 2.2.3.
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
    @Value("${andesstay.rabbitmq.queues.email-dlq}")
    private String dlqEmail;
    @Value("${andesstay.rabbitmq.queues.housekeeping}")
    private String colaHousekeeping;
    @Value("${andesstay.rabbitmq.queues.housekeeping-dlq}")
    private String dlqHousekeeping;
    @Value("${andesstay.rabbitmq.queues.voucher}")
    private String colaVoucher;
    @Value("${andesstay.rabbitmq.queues.voucher-dlq}")
    private String dlqVoucher;

    @GetMapping("/status")
    @PreAuthorize("hasRole('Admin')")
    public Map<String, Object> estado() {
        Map<String, Object> colas = new LinkedHashMap<>();
        for (String nombre : List.of(colaEmail, dlqEmail,
                colaHousekeeping, dlqHousekeeping,
                colaVoucher, dlqVoucher)) {
            colas.put(nombre, describir(nombre));
        }

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("servicio", "ms-andesstay-notify");
        respuesta.put("timestamp", Instant.now().toString());
        respuesta.put("colas", colas);
        respuesta.put("tasaDlq", metrics.tasaDlq());
        respuesta.put("eventIdEnRegistro", procesados.tamano());
        return respuesta;
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
