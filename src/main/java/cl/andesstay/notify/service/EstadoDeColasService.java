package cl.andesstay.notify.service;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * La profundidad de las nueve colas del servicio, agrupadas por flujo, leida del broker.
 *
 * POR QUE UN SERVICIO. Antes NotifyStatusController inyectaba AmqpAdmin y consultaba las colas el
 * mismo: la capa web conocia la infraestructura de mensajeria. Aqui queda todo lo que habla con el
 * broker —los nombres de las colas y la consulta— y el controlador solo arma la respuesta HTTP. El
 * contenido de esa respuesta no cambia.
 *
 * Las colas se agrupan por flujo y no en una lista plana, porque el conteo importa: tres flujos con
 * tres colas cada uno se lee como un patron repetido, mientras que nueve nombres sueltos invitan a
 * preguntar por que no son seis.
 *
 * La profundidad de las colas de espera es una medida que el rediseño hizo posible: mensajes con
 * reintento pendiente AHORA MISMO. Con el reintento en proceso de Spring Retry no existia, porque la
 * espera vivia en un hilo dormido y no en el broker.
 */
@Service
public class EstadoDeColasService {

    /**
     * Se inyecta la INTERFAZ, no RabbitAdmin.
     *
     * Spring Boot registra el bean con el tipo declarado AmqpAdmin, asi que pedir un RabbitAdmin
     * falla en el arranque con "No qualifying bean of type RabbitAdmin available", aunque la
     * instancia concreta sea exactamente esa. getQueueProperties pertenece a la interfaz y devuelve
     * el conteo de mensajes y de consumidores, que es todo lo que hace falta aqui.
     */
    private final AmqpAdmin amqpAdmin;

    private final String colaEmail;
    private final String esperaEmail;
    private final String dlqEmail;
    private final String colaHousekeeping;
    private final String esperaHousekeeping;
    private final String dlqHousekeeping;
    private final String colaVoucher;
    private final String esperaVoucher;
    private final String dlqVoucher;

    public EstadoDeColasService(
            AmqpAdmin amqpAdmin,
            @Value("${andesstay.rabbitmq.queues.email}") String colaEmail,
            @Value("${andesstay.rabbitmq.queues.email-retry}") String esperaEmail,
            @Value("${andesstay.rabbitmq.queues.email-dlq}") String dlqEmail,
            @Value("${andesstay.rabbitmq.queues.housekeeping}") String colaHousekeeping,
            @Value("${andesstay.rabbitmq.queues.housekeeping-retry}") String esperaHousekeeping,
            @Value("${andesstay.rabbitmq.queues.housekeeping-dlq}") String dlqHousekeeping,
            @Value("${andesstay.rabbitmq.queues.voucher}") String colaVoucher,
            @Value("${andesstay.rabbitmq.queues.voucher-retry}") String esperaVoucher,
            @Value("${andesstay.rabbitmq.queues.voucher-dlq}") String dlqVoucher) {
        this.amqpAdmin = amqpAdmin;
        this.colaEmail = colaEmail;
        this.esperaEmail = esperaEmail;
        this.dlqEmail = dlqEmail;
        this.colaHousekeeping = colaHousekeeping;
        this.esperaHousekeeping = esperaHousekeeping;
        this.dlqHousekeeping = dlqHousekeeping;
        this.colaVoucher = colaVoucher;
        this.esperaVoucher = esperaVoucher;
        this.dlqVoucher = dlqVoucher;
    }

    /** Los tres flujos, en el orden en que se muestran: email, housekeeping y voucher. */
    public Map<String, Object> flujos() {
        Map<String, Object> flujos = new LinkedHashMap<>();
        flujos.put("email", describirFlujo(colaEmail, esperaEmail, dlqEmail));
        flujos.put("housekeeping", describirFlujo(colaHousekeeping, esperaHousekeeping, dlqHousekeeping));
        flujos.put("voucher", describirFlujo(colaVoucher, esperaVoucher, dlqVoucher));
        return flujos;
    }

    /**
     * Las tres colas de un flujo, con el papel de cada una explicito en la clave.
     *
     * "espera" y no "retry" porque es lo que la cola hace: el mensaje aguarda ahi su siguiente
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
                // La cola no existe en el broker. Con la topologia declarada en codigo no deberia
                // pasar, asi que vale la pena que se vea en la respuesta en lugar de un 0 engañoso.
                return "no existe en el broker";
            }
            return Map.of(
                    "mensajes", info.get(RabbitAdmin.QUEUE_MESSAGE_COUNT),
                    "consumidores", info.get(RabbitAdmin.QUEUE_CONSUMER_COUNT));
        } catch (Exception e) {
            // Un broker caido no tiene que tumbar el endpoint de estado: justo entonces es cuando
            // mas se necesita ver que esta pasando.
            return "no consultable: " + e.getMessage();
        }
    }
}
