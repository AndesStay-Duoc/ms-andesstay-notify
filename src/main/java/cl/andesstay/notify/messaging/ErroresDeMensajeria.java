package cl.andesstay.notify.messaging;

import cl.andesstay.notify.service.SolicitudInvalidaException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

/**
 * Traduce los errores de los servicios a decisiones sobre el mensaje.
 *
 * Es el único punto donde un error de negocio se convierte en algo de RabbitMQ:
 *
 *   SolicitudInvalidaException (permanente) -> AmqpRejectAndDontRequeueException: el contenedor
 *       rechaza sin reencolar y sin pasar por la política de reintentos (RabbitMQConfig la excluye),
 *       y el mensaje va a su DLQ por el x-dead-letter-exchange de la cola.
 *   cualquier otra excepción (transitoria)   -> sale tal cual: el interceptor de reintentos la
 *       reintenta y, agotados los intentos, el mensaje también termina en la DLQ.
 */
public final class ErroresDeMensajeria {

    private ErroresDeMensajeria() {
    }

    public static Runnable permanenteComoRechazo(Runnable trabajo) {
        return () -> {
            try {
                trabajo.run();
            } catch (SolicitudInvalidaException e) {
                throw new AmqpRejectAndDontRequeueException(e.getMessage(), e);
            }
        };
    }
}
