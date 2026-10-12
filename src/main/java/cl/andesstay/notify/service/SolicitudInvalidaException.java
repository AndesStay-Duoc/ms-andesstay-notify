package cl.andesstay.notify.service;

/**
 * La solicitud no se puede cumplir tal como viene: destinatario inválido, plantilla o tipo de tarea
 * desconocidos, un dato obligatorio que falta.
 *
 * Es un error PERMANENTE: repetir la solicitud no la arregla. Los servicios lo lanzan sin saber
 * nada de RabbitMQ; quien decide qué significa para el mensaje (rechazarlo sin reintentar, rumbo a
 * la DLQ) es el consumidor. Ver messaging/ErroresDeMensajeria.
 */
public class SolicitudInvalidaException extends RuntimeException {

    public SolicitudInvalidaException(String mensaje) {
        super(mensaje);
    }
}
