package cl.andesstay.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología de RabbitMQ del caso AndesStay: tres exchanges, nueve colas y nueve bindings.
 *
 * Hasta ahora la topología existía únicamente en infra/mq/rabbitmq-definitions.json. Declararla
 * aquí la vuelve la misma en los tres entornos y hace que el servicio se autorrepare: si una cola
 * no existe, RabbitAdmin la crea al arrancar en lugar de dejar al listener fallando con NOT_FOUND.
 *
 * Las colas y los bindings se declaran SOLO en este servicio, que es quien consume. Los exchanges
 * se declaran además en ms-andesstay-reservations: declarar un exchange dos veces con el mismo
 * tipo y la misma durabilidad es idempotente, mientras que declarar una cola con argumentos
 * distintos falla con PRECONDITION_FAILED y cierra el canal. ms-andesstay-mq, que administra la
 * topología por HTTP, tampoco declara nada por su cuenta al arrancar, por el mismo motivo.
 *
 * TRES COLAS POR FLUJO. Cada flujo tiene una cola de trabajo, una cola de espera para el backoff
 * del reintento y una cola de mensajes muertos. La de espera no es ni una cola de trabajo —no
 * tiene consumidor— ni una DLQ —no es el final de ningún camino, su dead letter apunta hacia
 * atrás—: es el temporizador del backoff. RabbitMQ no tiene entrega diferida nativa, así que la
 * única forma de esperar sin ocupar un consumidor es un TTL más un dead letter exchange, y eso
 * necesita un sitio donde el mensaje espere.
 *
 * EL CONTEO DE EXCHANGES Y BINDINGS NO CAMBIA, y es intencionado. A la cola de espera se publica
 * por el exchange por omisión, que mantiene un binding implícito a cada cola por su nombre, así
 * que no hace falta declarar ni un exchange ni un binding nuevos. Un exchange cmd.retry con tres
 * bindings habría dejado la topología en cuatro exchanges y doce bindings, y toda la documentación
 * del proyecto afirma tres y nueve.
 *
 * IMPORTANTE al desplegar sobre un broker que ya tenga las colas creadas: una cola existente con
 * argumentos distintos hace que la declaración falle con "inequivalent arg". Las tres colas .retry
 * son nuevas y no chocan con nada, pero el procedimiento de infra/docs/validacion/rabbitmq.md
 * sigue valiendo para las de trabajo.
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

    // Ya NO hay un RabbitRetryTemplateCustomizer.
    //
    // Existía para excluir AmqpRejectAndDontRequeueException de la política de reintentos, porque
    // el SimpleRetryPolicy que Spring Boot armaba desde spring.rabbitmq.listener.simple.retry
    // reintentaba CUALQUIER excepción, incluida esa, cuya intención es exactamente la contraria.
    //
    // Ese interceptor ya no existe: el reintento lo hace el broker con la cola de espera, y la
    // clasificación entre error transitorio y permanente la hace el orden de los catch del
    // consumidor, que es más explícito y además se puede probar sin broker. Dejar el bean habría
    // sido código muerto, y además la señal más fuerte posible de que el servicio sigue
    // reintentando con Spring Retry.

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
    // Un Declarables por flujo en lugar de veintisiete @Bean sueltos: agrupa la cola de trabajo,
    // su cola de espera, su DLQ y sus tres bindings, que es la unidad que de verdad se razona
    // junta.

    @Bean
    public Declarables flujoEmail(
            @Value("${andesstay.rabbitmq.queues.email}") String cola,
            @Value("${andesstay.rabbitmq.queues.email-retry}") String colaReintento,
            @Value("${andesstay.rabbitmq.queues.email-dlq}") String dlq,
            @Value("${andesstay.rabbitmq.routing.email-send}") String rkDirect,
            @Value("${andesstay.rabbitmq.routing.email-topic}") String rkTopic,
            @Value("${andesstay.rabbitmq.routing.email-dead}") String rkDead) {
        return declararFlujo(cola, colaReintento, dlq, rkDirect, rkTopic, rkDead);
    }

    @Bean
    public Declarables flujoHousekeeping(
            @Value("${andesstay.rabbitmq.queues.housekeeping}") String cola,
            @Value("${andesstay.rabbitmq.queues.housekeeping-retry}") String colaReintento,
            @Value("${andesstay.rabbitmq.queues.housekeeping-dlq}") String dlq,
            @Value("${andesstay.rabbitmq.routing.housekeeping-ticket}") String rkDirect,
            @Value("${andesstay.rabbitmq.routing.housekeeping-topic}") String rkTopic,
            @Value("${andesstay.rabbitmq.routing.housekeeping-dead}") String rkDead) {
        return declararFlujo(cola, colaReintento, dlq, rkDirect, rkTopic, rkDead);
    }

    @Bean
    public Declarables flujoVoucher(
            @Value("${andesstay.rabbitmq.queues.voucher}") String cola,
            @Value("${andesstay.rabbitmq.queues.voucher-retry}") String colaReintento,
            @Value("${andesstay.rabbitmq.queues.voucher-dlq}") String dlq,
            @Value("${andesstay.rabbitmq.routing.voucher-gen}") String rkDirect,
            @Value("${andesstay.rabbitmq.routing.voucher-topic}") String rkTopic,
            @Value("${andesstay.rabbitmq.routing.voucher-dead}") String rkDead) {
        return declararFlujo(cola, colaReintento, dlq, rkDirect, rkTopic, rkDead);
    }

    /**
     * Declara un flujo completo: la cola de trabajo, la cola de espera del reintento, la DLQ y los
     * tres bindings.
     *
     * La cola de trabajo lleva tres argumentos que corresponden a las tres razones por las que
     * RabbitMQ hace dead lettering: rechazo del consumidor (x-dead-letter-*), expiración
     * (x-message-ttl) y desborde de la cola (x-max-length). La DLQ no lleva dead letter propio
     * porque es el final del camino, solo un TTL de retención para que el histórico no crezca sin
     * límite.
     */
    private Declarables declararFlujo(String cola, String colaReintento, String dlq,
                                      String rkDirect, String rkTopic, String rkDead) {

        Queue colaTrabajo = QueueBuilder.durable(cola)
                .withArgument("x-dead-letter-exchange", exchangeDlx)
                .withArgument("x-dead-letter-routing-key", rkDead)
                .withArgument("x-message-ttl", mensajeTtlMs)
                .withArgument("x-max-length", maxLength)
                .build();

        Queue colaEspera = colaDeEspera(colaReintento, rkDirect);

        Queue colaMuertos = QueueBuilder.durable(dlq)
                .withArgument("x-message-ttl", dlqTtlMs)
                .build();

        Binding porRoutingKeyExacta = BindingBuilder
                .bind(colaTrabajo).to(cmdDirectExchange()).with(rkDirect);

        Binding porPatron = BindingBuilder
                .bind(colaTrabajo).to(cmdTopicExchange()).with(rkTopic);

        Binding haciaLaDlq = BindingBuilder
                .bind(colaMuertos).to(cmdDeadLetterExchange()).with(rkDead);

        // La cola de espera NO lleva binding: a ella se publica por el exchange por omisión, que
        // mantiene uno implícito a cada cola por su nombre.
        return new Declarables(colaTrabajo, colaEspera, colaMuertos,
                porRoutingKeyExacta, porPatron, haciaLaDlq);
    }

    /**
     * La cola donde el mensaje espera su siguiente intento.
     *
     * Su dead letter apunta HACIA ATRÁS, a la cola de trabajo: cuando el mensaje vence, el broker
     * lo devuelve a cmd.direct con la misma routing key del binding de la cola de trabajo. La
     * routing key sale de la misma propiedad con la que se declara ese binding, en este mismo
     * método, así que las dos no pueden desincronizarse. Importa, porque un dead lettering que no
     * encuentra ningún binding se DESCARTA EN SILENCIO: el reintento desaparecería sin rastro.
     *
     * Las tres cosas que esta cola NO lleva son tan deliberadas como las que lleva:
     *
     * SIN x-message-ttl. El plazo va por mensaje, en la propiedad `expiration`, y lo pone
     * ReintentoPublisher según el peldaño del backoff. Si la cola llevara TTL propio, RabbitMQ
     * aplicaría el MÍNIMO de los dos y la escalera dejaría de funcionar por encima de ese valor.
     *
     * SIN consumidor. Lo único que puede sacar un mensaje de aquí es el vencimiento de su propio
     * plazo. Una cola de espera con consumidor no es una cola de espera.
     *
     * SIN x-max-length. Al desbordar, la política por omisión es drop-head y el mensaje expulsado
     * se dead-letterea con reason=maxlen por el dead letter exchange de la cola, que apunta a la
     * cola de trabajo: el desborde DEVOLVERÍA el mensaje al trabajo antes de terminar su espera.
     * Es seguro —no se pierde, solo se reintenta antes de tiempo— pero sorprendente, y la
     * alternativa x-overflow: reject-publish sería peor, porque rechazaría nuestra publicación sin
     * que el consumidor se enterase y el original se confirmaría igual. La población natural de
     * esta cola está acotada por el throughput multiplicado por el peldaño más largo, así que el
     * límite no hace falta.
     *
     * Y una propiedad que SÍ importa por lo que no ocurre: como la entrada a esta cola es un
     * basicPublish y no un dead lettering, no se forma ningún ciclo de dead letter. RabbitMQ
     * descarta en silencio un mensaje que sería dead-leterado hacia una cola por la que ya pasó si
     * en el ciclo no hubo ninguna expiración por TTL; este diseño no roza esa regla en ningún
     * momento, mientras que la variante «entrar por dead lettering» sí lo haría.
     */
    private Queue colaDeEspera(String nombre, String rkDeLaColaDeTrabajo) {
        return QueueBuilder.durable(nombre)
                .withArgument("x-dead-letter-exchange", exchangeDirect)
                .withArgument("x-dead-letter-routing-key", rkDeLaColaDeTrabajo)
                .build();
    }
}
