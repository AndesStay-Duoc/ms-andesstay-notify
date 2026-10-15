# ms-andesstay-notify

Microservicio de **notificaciones** del sistema AndesStay. Consume las colas de RabbitMQ y
ejecuta los envíos asíncronos: correo o push al huésped, ticket de preparación a housekeeping y
generación del voucher.

Es el servicio que resuelve dos de los problemas del caso: que el huésped no recibía voucher
formal y que housekeeping se enteraba tarde.

## Responsabilidad

| Cola | Qué hace |
|---|---|
| `q.cmd.email` | Envía correo o push al huésped: confirmación, recordatorio de check-in, checkout |
| `q.cmd.housekeeping` | Emite el ticket de preparación o limpieza de la unidad |
| `q.cmd.voucher` | Genera el PDF del voucher de reserva o boleta |

Cada una tiene **dos colas mas**: una de espera para el backoff del reintento y una de mensajes
muertos. Nueve colas en total, en tres grupos de tres:

| Trabajo | Espera | Cuarentena |
|---|---|---|
| `q.cmd.email` | `q.cmd.email.retry` | `q.cmd.email.dlq` |
| `q.cmd.housekeeping` | `q.cmd.housekeeping.retry` | `q.cmd.housekeeping.dlq` |
| `q.cmd.voucher` | `q.cmd.voucher.retry` | `q.cmd.voucher.dlq` |

La cola de espera no es ni una cola de trabajo --no tiene consumidor-- ni una DLQ --no es el final
de nada, su dead letter apunta hacia atras--: es el temporizador del backoff. RabbitMQ no tiene
entrega diferida nativa, asi que la unica forma de esperar sin ocupar un consumidor es un TTL mas un
dead letter exchange, y eso necesita un sitio donde el mensaje espere.

**Los exchanges y los bindings siguen siendo tres y nueve.** A la cola de espera se publica por el
exchange por omision, que mantiene un binding implicito a cada cola por su nombre, asi que no hay
nada nuevo que declarar.

**No expone endpoints públicos.** Es un consumidor puro y no tiene base de datos.

## Garantías

- **ACK y NACK explícitos.** `ackMode = "MANUAL"` en los seis `@RabbitListener`, con el canal como
  parámetro: el contenedor no confirma nada por su cuenta. Los cinco caminos están en
  `consumer/ConsumidorDeTrabajo.java`.
- **Idempotencia por `eventId`**: un mensaje repetido se confirma sin reejecutar el efecto.
- **Reintentos con backoff en el broker**, no en proceso. Cuatro intentos con tres esperas de 1 s,
  2 s y 4 s por omisión, y solo entonces la DLQ.
- **Cuatro métricas** por Actuator, incluida la tasa de DLQ.

### Los cinco caminos de cada entrega

| # | Caso | Salida |
|---|---|---|
| 1 | Éxito | `basicAck` |
| 2 | Duplicado | `basicAck`, sin reejecutar |
| 3 | Error **permanente** | `basicNack(requeue=false)`, a la DLQ sin gastar intentos |
| 4 | Transitorio con intentos | `basicPublish` a la cola de espera **y después** `basicAck` |
| 5 | Transitorio agotado | `basicNack(requeue=false)`, a la DLQ |

El orden del camino 4 no es negociable: al revés --confirmar y luego publicar-- una caída entre las
dos operaciones **pierde** el mensaje; en este orden deja un duplicado, que es lo que la
idempotencia existe para absorber.

## Stack

Java 17 · Spring Boot 3.2.5 · Spring AMQP · Actuator.

## Variables de entorno

| Variable | Descripción |
|---|---|
| `RABBITMQ_HOST` | Host del clúster RabbitMQ |
| `RABBITMQ_PORT` | Puerto AMQP, por defecto `5672` |
| `RABBITMQ_USER` | Usuario |
| `RABBITMQ_PASSWORD` | Contraseña |
| `NOTIFY_BACKOFF_LADDER` | Escalera de esperas en ms. Tres esperas significan cuatro intentos |

Se configuran en un archivo `.env` que **no se versiona**. Ver `.env.example`.

## Cómo levantarlo

```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev
```

Requiere RabbitMQ en marcha. El compose y la topología están en el repositorio
[`infra`](https://github.com/AndesStay-Duoc/infra).

## Contratos

Los exchanges, bindings y esquemas de mensaje son canónicos y viven en
[`infra/docs/contracts/events/rabbit.md`](https://github.com/AndesStay-Duoc/infra/blob/develop/docs/contracts/events/rabbit.md).

## Cómo contribuir

Ver [`CONTRIBUTING.md`](CONTRIBUTING.md).
