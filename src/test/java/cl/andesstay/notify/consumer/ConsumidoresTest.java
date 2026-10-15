package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.CorreoService;
import cl.andesstay.notify.service.HousekeepingService;
import cl.andesstay.notify.service.SolicitudInvalidaException;
import cl.andesstay.notify.service.VoucherService;
import cl.andesstay.notify.support.NotifyMetrics;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import cl.andesstay.notify.support.ProcessedEventCache;
import cl.andesstay.notify.support.ReintentoPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.search.MeterNotFoundException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.mail.MailSendException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Lo que les queda a los consumidores: traducir el envelope a la solicitud del servicio y traducir
 * los errores del servicio a decisiones sobre el mensaje.
 *
 * Con el ACK explícito esas decisiones son llamadas sobre el canal, así que se comprueban ahí:
 * basicAck al terminar, basicNack sin reencolar (a la DLQ) para un error permanente, y una copia en
 * la cola de espera para uno transitorio. Igual que en {@link ConsumidorDeTrabajoTest}, el registro
 * de idempotencia, las métricas y el publicador de reintentos son los reales y solo se simulan el
 * canal y los servicios: lo que se prueba es cómo encaja cada consumidor con ellos.
 */
class ConsumidoresTest {

    private static final long TAG = 11L;
    private static final List<Long> ESCALERA = List.of(1000L, 2000L, 4000L);

    private final CorreoService correoService = mock(CorreoService.class);
    private final HousekeepingService housekeepingService = mock(HousekeepingService.class);
    private final VoucherService voucherService = mock(VoucherService.class);

    private Channel canal;
    private SimpleMeterRegistry registro;
    private EmailConsumer email;
    private HousekeepingConsumer housekeeping;
    private VoucherConsumer voucher;

    @BeforeEach
    void preparar() {
        canal = mock(Channel.class);
        registro = new SimpleMeterRegistry();
        EnvelopeReader lector = new EnvelopeReader(new ObjectMapper());
        ProcesamientoIdempotente procesamiento = new ProcesamientoIdempotente(
                new ProcessedEventCache(Duration.ofHours(6), 50_000), new NotifyMetrics(registro));
        ReintentoPublisher reintentos = new ReintentoPublisher(ESCALERA);

        email = new EmailConsumer("cola-de-correos", "cola-de-correos.retry",
                correoService, lector, procesamiento, reintentos);
        housekeeping = new HousekeepingConsumer("cola-de-housekeeping", "cola-de-housekeeping.retry",
                housekeepingService, lector, procesamiento, reintentos);
        voucher = new VoucherConsumer("cola-de-vouchers", "cola-de-vouchers.retry",
                voucherService, lector, procesamiento, reintentos);
    }

    @Test
    void unErrorPermanenteDelServicioSeRechazaSinReencolar() throws Exception {
        doThrow(new SolicitudInvalidaException("destinatario inválido"))
                .when(correoService).enviar(any(), anyString());

        email.processEmail(mensaje("""
                {"eventId":"e1","traceId":"t1","payload":{"destinatario":"x","plantilla":"RESERVA_CONFIRMADA"}}"""),
                canal);

        // Camino 3: directo a la DLQ en el primer intento, sin gastar reintentos.
        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
        verify(canal, never()).basicPublish(anyString(), anyString(), any(), any());
    }

    @Test
    void unErrorTransitorioDelServicioSeReprogramaParaQueSeReintente() throws Exception {
        doThrow(new MailSendException("SMTP caído")).when(correoService).enviar(any(), anyString());

        email.processEmail(mensaje("""
                {"eventId":"e2","traceId":"t2","payload":{"destinatario":"a@b.cl","plantilla":"RESERVA_CONFIRMADA"}}"""),
                canal);

        // Camino 4: la copia va a la cola de espera de ESTE consumidor y luego se confirma el original.
        verify(canal).basicPublish(eq(""), eq("cola-de-correos.retry"), any(), any());
        verify(canal).basicAck(TAG, false);
        verify(canal, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void elCorreoAceptaLosNombresDelFormatoAnteriorYUsaSuCola() throws Exception {
        email.processEmail(mensaje("""
                {"eventId":"e3","traceId":"t3","payload":{"to":"a@b.cl","subject":"Hola","body":"Texto","reservationId":7}}"""),
                canal);

        ArgumentCaptor<CorreoService.Solicitud> solicitud = ArgumentCaptor.forClass(CorreoService.Solicitud.class);
        verify(correoService).enviar(solicitud.capture(), eq("t3"));
        assertThat(solicitud.getValue().destinatario()).isEqualTo("a@b.cl");
        assertThat(solicitud.getValue().asunto()).isEqualTo("Hola");
        assertThat(solicitud.getValue().cuerpo()).isEqualTo("Texto");
        assertThat(solicitud.getValue().reservationId()).isEqualTo("7");

        verify(canal).basicAck(TAG, false);
        assertThat(procesados("cola-de-correos")).isEqualTo(1d);
    }

    @Test
    void elTicketTraduceLosCamposDelContratoYDelFormatoAnterior() throws Exception {
        housekeeping.processHousekeeping(mensaje("""
                {"eventId":"e4","traceId":"t4","payload":{"action":"CLEAN","date":"2026-11-04","unitId":3}}"""),
                canal);

        ArgumentCaptor<HousekeepingService.Ticket> ticket = ArgumentCaptor.forClass(HousekeepingService.Ticket.class);
        verify(housekeepingService).emitir(ticket.capture(), eq("t4"));
        assertThat(ticket.getValue().tipoTarea()).isEqualTo("CLEAN");
        assertThat(ticket.getValue().fechaRequerida()).isEqualTo("2026-11-04");

        verify(canal).basicAck(TAG, false);
        assertThat(procesados("cola-de-housekeeping")).isEqualTo(1d);
    }

    @Test
    void elVoucherRechazaSinReencolarUnDocumentoInvalido() throws Exception {
        doThrow(new SolicitudInvalidaException("tipoDocumento desconocido"))
                .when(voucherService).generar(any(), anyString());

        voucher.processVoucher(mensaje("""
                {"eventId":"e5","traceId":"t5","payload":{"reservationId":7,"guestEmail":"a@b.cl","tipoDocumento":"FACTURA"}}"""),
                canal);

        verify(canal).basicNack(TAG, false, false);
        verify(canal, never()).basicAck(anyLong(), anyBoolean());
        verify(canal, never()).basicPublish(anyString(), anyString(), any(), any());
    }

    private static Message mensaje(String json) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        return new Message(json.getBytes(StandardCharsets.UTF_8), props);
    }

    /** Mensajes procesados con éxito en una cola: la métrica lleva el nombre de la cola. */
    private double procesados(String cola) {
        try {
            return registro.get("notify.messages.processed").tag("queue", cola).counter().count();
        } catch (MeterNotFoundException e) {
            return 0d;
        }
    }
}
