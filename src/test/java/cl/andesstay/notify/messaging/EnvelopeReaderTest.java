package cl.andesstay.notify.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Un cuerpo ilegible o sin eventId es un error permanente: reintentarlo tres veces no lo vuelve
 * legible, así que tiene que viajar a la DLQ de inmediato con
 * {@link AmqpRejectAndDontRequeueException}, sin gastar la política de reintentos.
 */
class EnvelopeReaderTest {

    private final EnvelopeReader reader = new EnvelopeReader(new ObjectMapper());

    private Message mensaje(String cuerpo) {
        return MessageBuilder.withBody(cuerpo.getBytes(StandardCharsets.UTF_8)).build();
    }

    @Test
    @DisplayName("lee el envelope contratado completo")
    void leeElEnvelopeCompleto() {
        Envelope envelope = reader.leer(mensaje("""
                {
                  "type": "email.send",
                  "eventId": "9f1c3b62-4d7e-4a1f-8c22-0b6d5e2a7f13",
                  "timestamp": "2026-10-10T14:30:00Z",
                  "traceId": "3a7f1e90-2c44-4b8d-9e01-7f2b5c8a4d16",
                  "correlationId": "b81d4f27-6e93-4c05-a7f8-1d3e9b2c6a40",
                  "source": "ms-andesstay-reservations",
                  "actor": { "userId": "sub-1", "role": "RECEPCIONISTA", "tenant": "staff" },
                  "payload": {
                    "plantilla": "RESERVA_CONFIRMADA",
                    "destinatario": "huesped@example.cl",
                    "reservationId": "R-2026-000123",
                    "datos": { "nombreHuesped": "Ana Soto", "unidad": "Cabana Coigue" }
                  }
                }
                """));

        assertEquals("email.send", envelope.type());
        assertEquals("staff", envelope.actor().tenant());
        assertEquals("huesped@example.cl", envelope.texto("destinatario"));
        assertEquals("Ana Soto", envelope.datos().get("nombreHuesped"));
    }

    @Test
    @DisplayName("tolera campos desconocidos, como exige el contrato")
    void toleraCamposDesconocidos() {
        // "Un consumidor nunca falla por encontrar campos que no esperaba" (envelope.md).
        Envelope envelope = reader.leer(mensaje(
                "{\"eventId\":\"e1\",\"campoDelFuturo\":123,\"payload\":{\"to\":\"a@b.cl\"}}"));

        assertEquals("e1", envelope.eventId());
        assertEquals("a@b.cl", envelope.texto("to"));
    }

    @Test
    @DisplayName("acepta el payload antiguo y el contratado con la misma llamada")
    void aceptaLosDosFormatosDePayload() {
        // Durante la ventana en que notify ya está desplegado y reservations todavía no.
        Envelope antiguo = reader.leer(mensaje(
                "{\"eventId\":\"e1\",\"payload\":{\"to\":\"viejo@example.cl\"}}"));
        Envelope nuevo = reader.leer(mensaje(
                "{\"eventId\":\"e2\",\"payload\":{\"destinatario\":\"nuevo@example.cl\"}}"));

        assertEquals("viejo@example.cl", antiguo.texto("destinatario", "to"));
        assertEquals("nuevo@example.cl", nuevo.texto("destinatario", "to"));
    }

    @Test
    @DisplayName("un cuerpo que no es JSON es error permanente")
    void cuerpoIlegibleEsPermanente() {
        AmqpRejectAndDontRequeueException e = assertThrows(
                AmqpRejectAndDontRequeueException.class,
                () -> reader.leer(mensaje("esto no es json")));

        assertTrue(e.getMessage().startsWith("cuerpo ilegible"), e.getMessage());
    }

    @Test
    @DisplayName("un envelope sin eventId es error permanente")
    void sinEventIdEsPermanente() {
        // Sin clave de idempotencia el mensaje no se puede procesar de forma segura.
        AmqpRejectAndDontRequeueException e = assertThrows(
                AmqpRejectAndDontRequeueException.class,
                () -> reader.leer(mensaje("{\"type\":\"email.send\",\"payload\":{}}")));

        assertEquals("envelope sin eventId", e.getMessage());
    }

    @Test
    @DisplayName("el traceId ausente no rompe el log")
    void traceIdAusente() {
        Envelope envelope = reader.leer(mensaje("{\"eventId\":\"e1\"}"));

        assertNull(envelope.traceId());
        assertEquals("(sin traceId)", envelope.traceIdODesconocido());
        assertTrue(envelope.datos().isEmpty(), "sin payload, datos() tiene que ser vacío y no null");
    }
}
