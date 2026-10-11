package cl.andesstay.notify.config;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.boot.autoconfigure.amqp.RabbitRetryTemplateCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.policy.SimpleRetryPolicy;

import java.util.Map;

/**
 * Topología de RabbitMQ del caso AndesStay: tres exchanges, seis colas y nueve bindings.
 *
 * Hasta ahora la topología existía únicamente en infra/mq/rabbitmq-definitions.json. Declararla
 * aquí la vuelve la misma en los tres entornos y hace que el servicio se autorrepare: si una cola
 * no existe, RabbitAdmin la crea al arrancar en lugar de dejar al listener fallando con NOT_FOUND.
 *
 * Las colas y los bindings se declaran SOLO en este servicio, que es quien consume. Los exchanges
 * se declaran además en ms-andesstay-reservations: declarar un exchange dos veces con el mismo
 * tipo y la misma durabilidad es idempotente, mientras que declarar una cola con argumentos
 * distintos falla con PRECONDITION_FAILED y cierra el canal.
 *
 * IMPORTANTE al desplegar sobre un broker que ya tenga las colas creadas desde el archivo de
 * definiciones: esas colas no llevan x-message-ttl ni x-max-length, así que la declaración
 * fallaría con "inequivalent arg". Hay que borrarlas antes, o partir de un volumen limpio. El
 * procedimiento está en infra/docs/validacion/rabbitmq.md.
 */
@Configuration
public class RabbitMQConfig {

    private final String exchangeDirect;
    private final String exchangeTopic;
    private final String exchangeDlx;

    private final long mensajeTtlMs;
    private final long dlqTtlMs;
    private final int maxLength;

    public RabbitMQConfig(
            @Value("${andesstay.rabbitmq.exchange.direct}") String exchangeDirect,
            @Value("${andesstay.rabbitmq.exchange.topic}") String exchangeTopic,
            @Value("${andesstay.rabbitmq.exchange.dlx}") String exchangeDlx,
            @Value("${andesstay.rabbitmq.retencion.mensaje-ttl-ms}") long mensajeTtlMs,
            @Value("${andesstay.rabbitmq.retencion.dlq-ttl-ms}") long dlqTtlMs,
            @Value("${andesstay.rabbitmq.retencion.max-length}") int maxLength) {
        this.exchangeDirect = exchangeDirect;
        this.exchangeTopic = exchangeTopic;
        this.exchangeDlx = exchangeDlx;
        this.mensajeTtlMs = mensajeTtlMs;
        this.dlqTtlMs = dlqTtlMs;
        this.maxLength = maxLength;
    }

    /**
     * Excluye los errores permanentes de la política de reintentos.
     *
     * El interceptor que Spring Boot arma desde spring.rabbitmq.listener.simple.retry usa un
     * SimpleRetryPolicy que reintenta CUALQUIER excepción, incluida
     * AmqpRejectAndDontRequeueException. Verificado contra el broker: un cuerpo ilegible se
     * reintentaba tres veces antes de llegar a la DLQ, cuando la intención de esa excepción es
     * exactamente la contraria.
     *
     * Reintentar un error permanente no solo es inútil: con el backoff exponencial retiene el
     * consumidor varios segundos por mensaje y retrasa los que sí podrían procesarse. Ver la
     * distinción entre error transitorio y permanente en
     * infra/docs/contracts/events/rabbit.md.
     */
    @Bean
    public RabbitRetryTemplateCustomizer noReintentarErroresPermanentes(
            @Value("${spring.rabbitmq.listener.simple.retry.max-attempts:3}") int maxIntentos) {
        return (target, retryTemplate) -> {
            if (target != RabbitRetryTemplateCustomizer.Target.LISTENER) {
                return;
            }
            // El último true es el valor por omisión: todo lo que no esté en el mapa SÍ se
            // reintenta. Solo lo marcado como false queda fuera.
            Map<Class<? extends Throwable>, Boolean> clasificacion =
                    Map.of(AmqpRejectAndDontRequeueException.class, false);
            retryTemplate.setRetryPolicy(new SimpleRetryPolicy(maxIntentos, clasificacion, true, true));
        };
    }

    // ── Exchanges ────────────────────────────────────────────────────────────
    // Durables y sin auto-delete: su definición sobrevive al reinicio del broker.

    @Bean
    public DirectExchange cmdDirectExchange() {
        return new DirectExchange(exchangeDirect, true, false);
    }

    @Bean
    public TopicExchange cmdTopicExchange() {
        return new TopicExchange(exchangeTopic, true, false);
    }

    /** Recibe los mensajes agotados o expirados de las tres colas de trabajo. */
    @Bean
    public DirectExchange cmdDeadLetterExchange() {
        return new DirectExchange(exchangeDlx, true, false);
    }

    // ── Flujos ───────────────────────────────────────────────────────────────
    // Un Declarables por flujo en lugar de dieciocho @Bean sueltos: agrupa la cola, su DLQ y sus
    // cuatro bindings, que es la unidad que de verdad se razona junta.

    @Bean
    public Declarables flujoEmail(
            @Value("${andesstay.rabbitmq.queues.email}") String cola,
            @Value("${andesstay.rabbitmq.queues.email-dlq}") String dlq,
            @Value("${andesstay.rabbitmq.routing.email-send}") String rkDirect,
            @Value("${andesstay.rabbitmq.routing.email-topic}") String rkTopic,
            @Value("${andesstay.rabbitmq.routing.email-dead}") String rkDead) {
        return declararFlujo(cola, dlq, rkDirect, rkTopic, rkDead);
    }

    @Bean
    public Declarables flujoHousekeeping(
            @Value("${andesstay.rabbitmq.queues.housekeeping}") String cola,
            @Value("${andesstay.rabbitmq.queues.housekeeping-dlq}") String dlq,
            @Value("${andesstay.rabbitmq.routing.housekeeping-ticket}") String rkDirect,
            @Value("${andesstay.rabbitmq.routing.housekeeping-topic}") String rkTopic,
            @Value("${andesstay.rabbitmq.routing.housekeeping-dead}") String rkDead) {
        return declararFlujo(cola, dlq, rkDirect, rkTopic, rkDead);
    }

    @Bean
    public Declarables flujoVoucher(
            @Value("${andesstay.rabbitmq.queues.voucher}") String cola,
            @Value("${andesstay.rabbitmq.queues.voucher-dlq}") String dlq,
            @Value("${andesstay.rabbitmq.routing.voucher-gen}") String rkDirect,
            @Value("${andesstay.rabbitmq.routing.voucher-topic}") String rkTopic,
            @Value("${andesstay.rabbitmq.routing.voucher-dead}") String rkDead) {
        return declararFlujo(cola, dlq, rkDirect, rkTopic, rkDead);
    }

    /**
     * Declara un flujo completo: la cola de trabajo, su DLQ y los cuatro bindings.
     *
     * La cola de trabajo lleva tres argumentos que corresponden a las tres razones por las que
     * RabbitMQ hace dead lettering: rechazo del consumidor (x-dead-letter-*), expiración
     * (x-message-ttl) y desborde de la cola (x-max-length). La DLQ no lleva dead letter propio
     * porque es el final del camino, solo un TTL de retención para que el histórico no crezca sin
     * límite.
     */
    private Declarables declararFlujo(String cola, String dlq,
                                      String rkDirect, String rkTopic, String rkDead) {

        Queue colaTrabajo = QueueBuilder.durable(cola)
                .withArgument("x-dead-letter-exchange", exchangeDlx)
                .withArgument("x-dead-letter-routing-key", rkDead)
                .withArgument("x-message-ttl", mensajeTtlMs)
                .withArgument("x-max-length", maxLength)
                .build();

        Queue colaMuertos = QueueBuilder.durable(dlq)
                .withArgument("x-message-ttl", dlqTtlMs)
                .build();

        Binding porRoutingKeyExacta = BindingBuilder
                .bind(colaTrabajo).to(cmdDirectExchange()).with(rkDirect);

        Binding porPatron = BindingBuilder
                .bind(colaTrabajo).to(cmdTopicExchange()).with(rkTopic);

        Binding haciaLaDlq = BindingBuilder
                .bind(colaMuertos).to(cmdDeadLetterExchange()).with(rkDead);

        return new Declarables(colaTrabajo, colaMuertos, porRoutingKeyExacta, porPatron, haciaLaDlq);
    }
}
