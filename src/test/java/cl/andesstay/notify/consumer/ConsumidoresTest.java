package cl.andesstay.notify.consumer;

import cl.andesstay.notify.messaging.EnvelopeReader;
import cl.andesstay.notify.service.CorreoService;
import cl.andesstay.notify.service.HousekeepingService;
import cl.andesstay.notify.service.SolicitudInvalidaException;
import cl.andesstay.notify.service.VoucherService;
import cl.andesstay.notify.support.ProcesamientoIdempotente;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.mail.MailSendException;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Lo que les queda a los consumidores: traducir el envelope a la solicitud del servicio y traducir
 * los errores del servicio a decisiones sobre el mensaje.
 */
class ConsumidoresTest {

    private final EnvelopeReader reader = new EnvelopeReader(new ObjectMapper());
    private final ProcesamientoIdempotente procesamiento = mock(ProcesamientoIdempotente.class);
    private final CorreoService correoService = mock(CorreoService.class);
    private final HousekeepingService housekeepingService = mock(HousekeepingService.class);
    private final VoucherService voucherService = mock(VoucherService.class);

    private final EmailConsumer email = new EmailConsumer(reader, procesamiento, correoService);
    private final HousekeepingConsumer housekeeping = new HousekeepingConsumer(reader, procesamiento, housekeepingService);
    private final VoucherConsumer voucher = new VoucherConsumer(reader, procesamiento, voucherService);

    @BeforeEach
    void preparar() {
        ReflectionTestUtils.setField(email, "cola", "cola-de-correos");
        ReflectionTestUtils.setField(housekeeping, "cola", "cola-de-housekeeping");
        ReflectionTestUtils.setField(voucher, "cola", "cola-de-vouchers");
        // El registro de idempotencia real se prueba aparte; aquí solo ejecuta el trabajo.
        doAnswer(inv -> {
            ((Runnable) inv.getArgument(3)).run();
            return null;
        }).when(procesamiento).ejecutar(anyString(), anyString(), anyString(), any(Runnable.class));
    }

    @Test
    void unErrorPermanenteDelServicioSeRechazaSinReencolar() {
        doThrow(new SolicitudInvalidaException("destinatario inválido"))
                .when(correoService).enviar(any(), anyString());

        assertThatThrownBy(() -> email.processEmail(mensaje("""
                {"eventId":"e1","traceId":"t1","payload":{"destinatario":"x","plantilla":"RESERVA_CONFIRMADA"}}""")))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class)
                .hasMessage("destinatario inválido")
                .hasCauseInstanceOf(SolicitudInvalidaException.class);
    }

    @Test
    void unErrorTransitorioDelServicioSalePorDondeVinoParaQueSeReintente() {
        doThrow(new MailSendException("SMTP caído")).when(correoService).enviar(any(), anyString());

        assertThatThrownBy(() -> email.processEmail(mensaje("""
                {"eventId":"e2","traceId":"t2","payload":{"destinatario":"a@b.cl","plantilla":"RESERVA_CONFIRMADA"}}""")))
                .isInstanceOf(MailSendException.class);
    }

    @Test
    void elCorreoAceptaLosNombresDelFormatoAnteriorYUsaSuCola() {
        email.processEmail(mensaje("""
                {"eventId":"e3","traceId":"t3","payload":{"to":"a@b.cl","subject":"Hola","body":"Texto","reservationId":7}}"""));

        ArgumentCaptor<CorreoService.Solicitud> solicitud = ArgumentCaptor.forClass(CorreoService.Solicitud.class);
        verify(correoService).enviar(solicitud.capture(), eq("t3"));
        assertThat(solicitud.getValue().destinatario()).isEqualTo("a@b.cl");
        assertThat(solicitud.getValue().asunto()).isEqualTo("Hola");
        assertThat(solicitud.getValue().cuerpo()).isEqualTo("Texto");
        assertThat(solicitud.getValue().reservationId()).isEqualTo("7");
        verify(procesamiento).ejecutar(eq("cola-de-correos"), eq("e3"), eq("t3"), any(Runnable.class));
    }

    @Test
    void elTicketTraduceLosCamposDelContratoYDelFormatoAnterior() {
        housekeeping.processHousekeeping(mensaje("""
                {"eventId":"e4","traceId":"t4","payload":{"action":"CLEAN","date":"2026-11-04","unitId":3}}"""));

        ArgumentCaptor<HousekeepingService.Ticket> ticket = ArgumentCaptor.forClass(HousekeepingService.Ticket.class);
        verify(housekeepingService).emitir(ticket.capture(), eq("t4"));
        assertThat(ticket.getValue().tipoTarea()).isEqualTo("CLEAN");
        assertThat(ticket.getValue().fechaRequerida()).isEqualTo("2026-11-04");
        verify(procesamiento).ejecutar(eq("cola-de-housekeeping"), eq("e4"), eq("t4"), any(Runnable.class));
    }

    @Test
    void elVoucherRechazaSinReencolarUnDocumentoInvalido() {
        doThrow(new SolicitudInvalidaException("tipoDocumento desconocido"))
                .when(voucherService).generar(any(), anyString());

        assertThatThrownBy(() -> voucher.processVoucher(mensaje("""
                {"eventId":"e5","traceId":"t5","payload":{"reservationId":7,"guestEmail":"a@b.cl","tipoDocumento":"FACTURA"}}""")))
                .isInstanceOf(AmqpRejectAndDontRequeueException.class);
    }

    private static Message mensaje(String json) {
        return new Message(json.getBytes(StandardCharsets.UTF_8), new MessageProperties());
    }
}
