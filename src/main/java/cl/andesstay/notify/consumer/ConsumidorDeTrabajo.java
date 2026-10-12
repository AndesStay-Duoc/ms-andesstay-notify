package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.Envelope;
import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import cl.andesstay.notify.support.ProcesamientoIdempotente.Resultado;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;

import java.io.IOException;

/**
 * Los cinco caminos del ACK manual, escritos una vez para los tres consumidores de trabajo.
 *
 * ACK MANUAL. El contenedor no confirma ni rechaza nada: cada entrega tiene que resolverse aquí
 * con un basicAck o un basicNack. No hay una sexta posibilidad, y conviene saber por qué: con
 * AcknowledgeMode.MANUAL, un método de listener que LANZA no produce un rechazo, deja el mensaje
 * sin confirmar hasta que el contenedor cierra el canal, y entonces el broker lo REENCOLA —no lo
 * dead-leterea—, lo que da un bucle de reentrega sin backoff en lugar de una cuarentena. De ahí el
 * catch(Throwable) final.
 *
 * ─── Los cinco caminos ───────────────────────────────────────────────────────────────────────────
 *
 *  1  ÉXITO                        basicAck
 *  2  DUPLICADO (mismo eventId)    basicAck             el efecto no se reejecuta
 *  3  ERROR PERMANENTE             basicNack(false)     DLQ ya, sin gastar intentos
 *  4  TRANSITORIO, quedan intentos basicPublish a la cola de espera + basicAck
 *  5  TRANSITORIO, agotados        basicNack(false)     DLQ
 *
 * El requeue=false de los caminos 3 y 5 es lo que activa el x-dead-letter-exchange de la cola de
 * trabajo y manda el mensaje a su DLQ por cmd.dead.dlx. Con requeue=true el mensaje volvería a la
 * cabeza de la misma cola y fallaría otra vez de inmediato: un bucle apretado, no un reintento.
 *
 * El camino 4 publica ANTES de confirmar, y por el mismo canal de la entrega. El orden no es
 * negociable: al revés —confirmar y luego publicar— una caída entre las dos operaciones PIERDE el
 * mensaje. En este orden, una caída deja un duplicado, que es lo que la idempotencia por eventId
 * existe para absorber. Si hiciera falta atomicidad, la respuesta sería un canal transaccional
 * (channelTransacted), que no se activa porque un viaje sincrónico al broker por mensaje no se
 * justifica para notificaciones.
 *
 * ─── Por qué esto es una clase base y no está copiado tres veces ────────────────────────────────
 *
 * El bloque de arriba es lógica delicada en la que un cambio inocente produce un fallo silencioso,
 * y triplicarla obligaría a corregir tres veces cada arreglo. Lo que SÍ está en cada consumidor,
 * porque es lo que define su contrato con el broker, es su propio
 * {@code @RabbitListener(..., ackMode = "MANUAL")} y su propio parámetro {@link Channel}: cada uno
 * declara que gestiona su acknowledge y recibe el canal con el que lo hace.
 */
@Slf4j
abstract class ConsumidorDeTrabajo {

    private static final String SIN_TRACE = "(sin traceId)";

    /** Nombre de la cola de trabajo. Sale de la misma propiedad que el @RabbitListener. */
    private final String cola;

    /** Nombre de la cola donde el mensaje espera su siguiente intento. */
    private final String colaDeEspera;

    /** Prefijo de los log de este flujo: Email, Housekeeping, Voucher. */
    private final String etiqueta;

    private final EnvelopeReader envelopeReader;
    private final ProcesamientoIdempotente procesamiento;
    private final ReintentoPublisher reintentos;

    protected ConsumidorDeTrabajo(String cola, String colaDeEspera, String etiqueta,
                                  EnvelopeReader envelopeReader,
                                  ProcesamientoIdempotente procesamiento,
                                  ReintentoPublisher reintentos) {
        this.cola = cola;
        this.colaDeEspera = colaDeEspera;
        this.etiqueta = etiqueta;
        this.envelopeReader = envelopeReader;
        this.procesamiento = procesamiento;
        this.reintentos = reintentos;
    }

    /**
     * El trabajo de negocio del flujo: enviar el correo, emitir el ticket, generar el documento.
     *
     * Clasificación de errores que esta clase espera de las implementaciones:
     *
     *   transitorio (SMTP caído, timeout)             cualquier excepción -> camino 4 o 5
     *   permanente  (destinatario o plantilla malos)  AmqpRejectAndDontRequeueException -> camino 3
     */
    protected abstract void procesar(Envelope envelope, String traceId);

    /**
     * Resuelve la entrega por uno de los cinco caminos.
     *
     * El canal llega como parámetro y NO se guarda en un campo: es el canal de ESTA entrega, y un
     * deliveryTag solo es válido en el canal que lo emitió. Confirmarlo en otro canal es un
     * PRECONDITION_FAILED que cierra el canal.
     */
    protected final void resolver(Message message, Channel channel) {

        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        int intento = reintentos.intentoActual(message);
        int maxIntentos = reintentos.maxIntentos();
        String traceId = SIN_TRACE;

        try {
            // envelopeReader.leer lanza AmqpRejectAndDontRequeueException con un cuerpo ilegible o
            // sin eventId, así que va DENTRO del try: es un error permanente como cualquier otro y
            // tiene que salir por el camino 3, no por el catch(Throwable).
            Envelope envelope = envelopeReader.leer(message);
            String trace = envelope.traceIdODesconocido();
            traceId = trace;

            Resultado resultado = procesamiento.ejecutar(
                    cola, envelope.eventId(), trace, intento, () -> procesar(envelope, trace));

            if (resultado == Resultado.DUPLICADO) {
                // ─── CAMINO 2 · DUPLICADO ──────────────────────────────────────────────────────
                // Se confirma igual que un éxito. El efecto ya se ejecutó en una entrega anterior
                // cuyo ack se perdió; reencolar o rechazar aquí sería tratar como fallo la
                // garantía «al menos una vez» funcionando como debe.
                log.info("[{}] ACK de duplicado tag={} traceId={}", etiqueta, deliveryTag, trace);
                confirmar(channel, deliveryTag, trace);
                return;
            }

            // ─── CAMINO 1 · ÉXITO ──────────────────────────────────────────────────────────────
            log.info("[{}] ACK tag={} intento={}/{} traceId={}",
                    etiqueta, deliveryTag, intento, maxIntentos, trace);
            confirmar(channel, deliveryTag, trace);

        } catch (AmqpRejectAndDontRequeueException e) {
            // ─── CAMINO 3 · ERROR PERMANENTE ───────────────────────────────────────────────────
            // Destinatario inválido, plantilla inexistente, cuerpo ilegible, falta el eventId.
            // Reintentarlo no lo va a arreglar: retendría el consumidor varios segundos y
            // llegaría al mismo sitio. NACK sin requeue en el primer intento.
            //
            // La excepción se conserva como marcador de «permanente» aunque con ACK manual el
            // contenedor ya no le dé ningún trato especial: quien la interpreta es este catch. Eso
            // deja intactos EnvelopeReader y EmailTemplates, y el nombre sigue describiendo lo que
            // ocurre.
            log.error("[{}] NACK sin requeue por error PERMANENTE → DLQ. tag={} traceId={} motivo={}",
                    etiqueta, deliveryTag, traceId, e.getMessage());
            rechazar(channel, deliveryTag, traceId);

        } catch (Exception e) {
            if (reintentos.quedanIntentos(intento)) {
                // ─── CAMINO 4 · TRANSITORIO CON REINTENTO PENDIENTE ────────────────────────────
                // SMTP caído, timeout de red. Se publica la copia con el contador incrementado y
                // el peldaño de backoff en `expiration`, y solo si la publicación salió bien se
                // confirma el original.
                try {
                    reintentos.programar(cola, colaDeEspera, message, intento, channel);
                } catch (Exception publicacion) {
                    // La copia no llegó al broker. NO se confirma el original: el contenedor
                    // cerrará el canal, el broker reentregará el mensaje sin confirmar con su
                    // cabecera de intentos intacta, y se reintentará desde el mismo punto. Se
                    // retrasa el reintento, no se pierde nada.
                    log.error("[{}] No se pudo programar el intento {}; el mensaje queda SIN "
                                    + "confirmar para que el broker lo reentregue. tag={} traceId={} motivo={}",
                            etiqueta, intento + 1, deliveryTag, traceId, publicacion.getMessage());
                    return;
                }
                log.warn("[{}] ACK del original tras programar el intento {}/{}. tag={} traceId={} motivo={}",
                        etiqueta, intento + 1, maxIntentos, deliveryTag, traceId, e.getMessage());
                confirmar(channel, deliveryTag, traceId);

            } else {
                // ─── CAMINO 5 · TRANSITORIO CON LOS INTENTOS AGOTADOS ──────────────────────────
                // Último fallo. NACK sin requeue: cmd.dead.dlx lleva el mensaje a la DLQ del
                // flujo, llega con x-andesstay-intentos al valor del último intento consumido y
                // con x-death[0].reason=rejected, y DeadLetterConsumer lo registra y mueve
                // notify.messages.dlq.
                log.error("[{}] NACK sin requeue: agotados los {} intentos → DLQ. tag={} traceId={} motivo={}",
                        etiqueta, maxIntentos, deliveryTag, traceId, e.getMessage());
                rechazar(channel, deliveryTag, traceId);
            }

        } catch (Throwable t) {
            // Red de seguridad. Con ACK manual, un método que lanza NO produce un nack: deja el
            // mensaje colgado hasta que se cierra el canal, y entonces se REENCOLA. Un
            // NoClassDefFoundError por aquí convertiría un mensaje en un bucle de reentrega
            // infinito. Se rechaza hacia la DLQ, que es un sitio del que se puede recuperar.
            log.error("[{}] NACK por error inesperado tag={} traceId={}",
                    etiqueta, deliveryTag, traceId, t);
            rechazar(channel, deliveryTag, traceId);

            if (t instanceof Error error) {
                // Un OutOfMemoryError no es recuperable y no debe quedar enterrado en un log: el
                // mensaje ya está en cuarentena, así que dejarlo subir es lo correcto.
                throw error;
            }
        }
    }

    /**
     * Un basicAck que falla significa canal o conexión caídos. No hay nada que hacer salvo
     * registrarlo: el mensaje sigue sin confirmar y el broker lo reentregará, y la idempotencia
     * por eventId es lo que impide que el huésped reciba la notificación dos veces. Si se dejara
     * propagar, el catch(Throwable) intentaría un nack por el mismo canal roto.
     */
    private void confirmar(Channel channel, long deliveryTag, String traceId) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (IOException e) {
            log.error("[{}] Falló el ACK tag={} traceId={}: {}. El broker reentregará; la "
                            + "idempotencia por eventId evita el efecto duplicado.",
                    etiqueta, deliveryTag, traceId, e.getMessage());
        }
    }

    private void rechazar(Channel channel, long deliveryTag, String traceId) {
        try {
            // multiple=false: solo esta entrega, no todas las pendientes del canal.
            // requeue=false: al dead letter exchange, no a la cabeza de la cola.
            channel.basicNack(deliveryTag, false, false);
        } catch (IOException e) {
            log.error("[{}] Falló el NACK tag={} traceId={}: {}. El mensaje se reentregará al "
                            + "reabrirse el canal y volverá a recorrer la misma clasificación.",
                    etiqueta, deliveryTag, traceId, e.getMessage());
        }
    }
}
