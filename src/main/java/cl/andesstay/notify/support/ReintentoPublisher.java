package cl.andesstay.notify.support;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Programa el siguiente intento de un mensaje que falló por una causa transitoria.
 *
 * El mecanismo es una cola de espera sin consumidor: se publica una copia del mensaje en
 * q.cmd.&lt;flujo&gt;.retry con la propiedad `expiration` puesta al peldaño de backoff que corresponda.
 * Nadie la consume, así que lo único que puede sacar al mensaje de ahí es el vencimiento de su
 * propio plazo, y entonces el x-dead-letter-exchange de esa cola lo devuelve a la cola de trabajo.
 *
 * POR QUÉ SE ENTRA PUBLICANDO Y SE SALE RECHAZANDO. Una cola de RabbitMQ admite UN SOLO
 * x-dead-letter-exchange y una sola routing key. Si la cola de trabajo dead-leterase hacia la cola
 * de espera, ya no podría dead-leterar hacia su DLQ, porque un basicNack(requeue=false) siempre
 * usa el dead letter de la cola y no uno que elija el consumidor. Las dos rutas diferenciadas que
 * el caso necesita —reintentar lo transitorio y poner en cuarentena lo agotado— son incompatibles
 * con un único dead letter por cola. Entrar con basicPublish y salir con basicNack es lo que
 * permite que ambas existan.
 *
 * Se publica por el exchange por omisión ("") con la routing key igual al nombre de la cola: el
 * binding implícito que RabbitMQ mantiene para cada cola evita declarar un exchange y tres
 * bindings, y la topología sigue siendo tres exchanges y nueve bindings.
 *
 * TTL POR MENSAJE Y NO POR COLA, a sabiendas de que eso expone al bloqueo de cabecera: con TTL por
 * mensaje RabbitMQ solo evalúa el vencimiento del mensaje que está EN LA CABEZA de la cola, así
 * que uno con expiration=4000 delante de otro con expiration=1000 hace esperar al segundo hasta
 * 4 s en lugar de 1 s. Se acepta por cuatro razones concretas: el error está acotado por el
 * peldaño más largo y no es una espera indefinida; solo ALARGA la espera, nunca la acorta, que sí
 * sería un defecto, porque el backoff es un mínimo y no una fecha límite; las colas son por flujo,
 * así que un pico de correo no retrasa los vouchers; y con prefetch 1 y el volumen del caso no hay
 * dos mensajes en reintento a la vez, que es lo que la condición requiere. La alternativa —tres
 * colas de TTL fijo por flujo— costaría quince colas en lugar de nueve y destruiría la lectura de
 * «tres flujos con su cola de trabajo y su DLQ» en el diagrama y en la Management UI.
 */
@Slf4j
@Component
public class ReintentoPublisher {

    /**
     * Intentos ya consumidos y fallados. Ausente equivale a 0, de modo que la primera entrega es
     * el intento 1.
     *
     * ES UNA CABECERA PROPIA Y NO LA x-death DEL BROKER, a propósito. x-death se identifica por el
     * par (queue, reason), así que un vencimiento por x-message-ttl y un desborde por
     * x-max-length sobre la MISMA cola producen entradas distintas con contadores distintos; su
     * entrada más reciente va en el índice 0 y no la primera; y cualquier republicación la
     * reinicia, porque lo que lleve el mensaje nuevo es lo que el publicador haya copiado. Un
     * entero bajo nuestro control vale igual en los cinco caminos del consumidor y se puede
     * afirmar en un test sin broker. x-death se sigue usando, pero solo como diagnóstico en
     * DeadLetterConsumer.
     */
    public static final String HEADER_INTENTOS = "x-andesstay-intentos";

    /**
     * El convertidor traduce las MessageProperties de Spring a las BasicProperties del cliente de
     * RabbitMQ, que es lo que acepta basicPublish. No tiene estado, así que una instancia basta.
     */
    private final MessagePropertiesConverter convertidor = new DefaultMessagePropertiesConverter();

    /** Escalera de esperas, una por reintento. Tres esperas significan cuatro intentos. */
    private final List<Long> esperasMs;

    public ReintentoPublisher(
            @Value("${andesstay.rabbitmq.reintento.esperas-ms}") List<Long> esperasMs) {
        if (esperasMs == null || esperasMs.isEmpty()) {
            // Arrancar con la escalera vacía dejaría un servicio que manda a la DLQ en el primer
            // fallo sin que nada lo advierta. Mejor no arrancar.
            throw new IllegalArgumentException("andesstay.rabbitmq.reintento.esperas-ms no puede "
                    + "estar vacía: sin esperas no hay reintentos");
        }
        this.esperasMs = List.copyOf(esperasMs);
        log.info("[Reintento] Escalera de backoff: {} ms → {} intentos por mensaje",
                this.esperasMs, maxIntentos());
    }

    /** Número de entrega actual: 1 para la primera. */
    public int intentoActual(Message mensaje) {
        Object valor = mensaje.getMessageProperties().getHeader(HEADER_INTENTOS);
        // El cliente puede entregar el entero como Integer, Long o Short según cómo viajara, de
        // ahí la comprobación contra Number y no contra Integer.
        int previos = (valor instanceof Number n) ? n.intValue() : 0;
        return previos + 1;
    }

    /** Cuatro intentos cuando hay tres esperas: el último ya no espera, va a la DLQ. */
    public int maxIntentos() {
        return esperasMs.size() + 1;
    }

    public boolean quedanIntentos(int intento) {
        return intento < maxIntentos();
    }

    /** La espera que toca después del intento indicado. */
    public long esperaTrasIntento(int intento) {
        return esperasMs.get(Math.min(intento - 1, esperasMs.size() - 1));
    }

    /**
     * Publica la copia del mensaje en la cola de espera.
     *
     * Se publica por el MISMO canal del que vino la entrega, y quien llama hace basicAck DESPUÉS.
     * El orden no es negociable: al revés —confirmar y luego publicar— una caída entre las dos
     * operaciones PIERDE el mensaje. En este orden, una caída deja un duplicado, que es
     * exactamente lo que la idempotencia por eventId existe para absorber. Y como el broker
     * procesa los frames de un canal en orden, la ventana es de microsegundos.
     */
    public void programar(String colaTrabajo, String colaEspera, Message original,
                          int intento, Channel canal) throws IOException {

        long espera = esperaTrasIntento(intento);
        MessageProperties copia = propiedadesDeLaCopia(original, intento, espera);

        canal.basicPublish("",
                colaEspera,
                convertidor.fromMessageProperties(copia, "UTF-8"),
                original.getBody());

        log.warn("[Reintento] {} · el intento {}/{} falló; el mensaje espera {} ms en {}",
                colaTrabajo, intento, maxIntentos(), espera, colaEspera);
    }

    /**
     * Las propiedades se construyen a mano en lugar de reutilizar las de la entrega, por tres
     * motivos.
     *
     * Uno: hay que QUITAR x-death y x-first-death-*, que son del broker. Copiarlas dejaría al
     * vencimiento actualizando un historial que nosotros propagamos a mano, y haría ilegible la
     * entrada de la DLQ, donde lo que importa es la última muerte y no un contador acumulado.
     *
     * Dos: hay que PONER `expiration`, y si el publicador original ya traía una, heredarla haría
     * vencer la copia antes de su peldaño. Hoy NotificationPublisher no pone expiration, pero el
     * diseño no debe depender de eso.
     *
     * Tres: delivery_mode PERSISTENT tiene que quedar explícito. La copia es un mensaje NUEVO, y
     * un mensaje transitorio en una cola durable no sobrevive al reinicio del broker, que es justo
     * el momento en que un reintento pendiente importa.
     */
    private MessageProperties propiedadesDeLaCopia(Message original, int intento, long espera) {
        MessageProperties origen = original.getMessageProperties();
        MessageProperties copia = new MessageProperties();

        copia.setContentType(origen.getContentType());
        copia.setContentEncoding(origen.getContentEncoding());
        copia.setCorrelationId(origen.getCorrelationId());
        copia.setMessageId(origen.getMessageId());
        copia.setType(origen.getType());
        copia.setDeliveryMode(MessageDeliveryMode.PERSISTENT);

        Map<String, Object> cabeceras = new LinkedHashMap<>(origen.getHeaders());
        cabeceras.keySet().removeIf(ReintentoPublisher::esDelBroker);
        cabeceras.forEach(copia::setHeader);

        copia.setHeader(HEADER_INTENTOS, intento);

        // La propiedad `expiration` de AMQP es una CADENA de milisegundos. Pasar un entero es el
        // error clásico: el broker la ignora y el mensaje se queda en la cola de espera para
        // siempre, sin ningún consumidor que lo saque.
        copia.setExpiration(String.valueOf(espera));

        return copia;
    }

    /** Cabeceras que escribe RabbitMQ y que no nos corresponde propagar. */
    private static boolean esDelBroker(String cabecera) {
        return "x-death".equals(cabecera) || cabecera.startsWith("x-first-death");
    }
}
