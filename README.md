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

Cada una tiene su cola de mensajes muertos: `q.cmd.email.dlq`, `q.cmd.housekeeping.dlq` y
`q.cmd.voucher.dlq`.

**No expone endpoints públicos.** Es un consumidor puro y no tiene base de datos.

## Garantías

- **ACK/NACK explícitos**: nada se confirma antes de procesarse.
- **Idempotencia por `eventId`**: un mensaje repetido no se procesa dos veces.
- **Reintentos con backoff** y, agotados los intentos, envío a la DLQ correspondiente.
- **Métrica de tasa de DLQ** expuesta por Actuator.

## Stack

Java 21 · Spring Boot 3.5 · Spring AMQP · Actuator.

## Variables de entorno

| Variable | Descripción |
|---|---|
| `RABBITMQ_HOST` | Host del clúster RabbitMQ |
| `RABBITMQ_PORT` | Puerto AMQP, por defecto `5672` |
| `RABBITMQ_USER` | Usuario |
| `RABBITMQ_PASSWORD` | Contraseña |
| `NOTIFY_MAX_ATTEMPTS` | Reintentos antes de enviar a la DLQ |

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
