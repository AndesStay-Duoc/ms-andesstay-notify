package cl.andesstay.notify.service;

import cl.andesstay.notify.email.EmailTemplates;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Los servicios de negocio, sin RabbitMQ de por medio: un error permanente es
 * SolicitudInvalidaException, uno transitorio sale tal cual.
 */
class ServiciosTest {

    private final JavaMailSender mailSender = mock(JavaMailSender.class);
    private final CorreoService correo = new CorreoService(mailSender, new EmailTemplates());
    private final HousekeepingService housekeeping = new HousekeepingService();
    private final VoucherService voucher = new VoucherService();

    private static final Map<String, Object> DATOS = Map.of("unitId", 1, "fechaEntrada", "2026-11-02");

    @BeforeEach
    void configurarRemitente() {
        ReflectionTestUtils.setField(correo, "fromAddress", "AndesStay <noreply@andesstay.cl>");
    }

    // ── Correo ───────────────────────────────────────────────────────────────

    @Test
    void enviaElCorreoConLaPlantillaRenderizada() {
        correo.enviar(new CorreoService.Solicitud("huesped@andesstay.test", "RESERVA_CONFIRMADA", "7", DATOS, null, null), "t");

        ArgumentCaptor<SimpleMailMessage> enviado = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(mailSender).send(enviado.capture());
        assertThat(enviado.getValue().getTo()).containsExactly("huesped@andesstay.test");
        assertThat(enviado.getValue().getFrom()).isEqualTo("AndesStay <noreply@andesstay.cl>");
        assertThat(enviado.getValue().getSubject()).contains("#7");
    }

    @Test
    void unDestinatarioInvalidoEsPermanenteYNoLlegaAlServidorDeCorreo() {
        assertThatThrownBy(() -> correo.enviar(
                new CorreoService.Solicitud("no-es-un-correo", "RESERVA_CONFIRMADA", "7", DATOS, null, null), "t"))
                .isInstanceOf(SolicitudInvalidaException.class)
                .hasMessageContaining("destinatario inválido 'no-es-un-correo'");
        verify(mailSender, never()).send(any(SimpleMailMessage.class));
    }

    @Test
    void sinPlantillaNiAsuntoEsPermanente() {
        assertThatThrownBy(() -> correo.enviar(
                new CorreoService.Solicitud("huesped@andesstay.test", null, "7", DATOS, null, null), "t"))
                .isInstanceOf(SolicitudInvalidaException.class);
    }

    @Test
    void unFalloDelServidorDeCorreoEsTransitorioYSaleSinTocar() {
        doThrow(new MailSendException("SMTP caído")).when(mailSender).send(any(SimpleMailMessage.class));

        assertThatThrownBy(() -> correo.enviar(
                new CorreoService.Solicitud("huesped@andesstay.test", null, "7", DATOS, "Asunto", "Cuerpo"), "t"))
                .isInstanceOf(MailSendException.class);
    }

    // ── Housekeeping ─────────────────────────────────────────────────────────

    @Test
    void emiteElTicketDePreparacion() {
        assertThatCode(() -> housekeeping.emitir(
                new HousekeepingService.Ticket("PREPARACION", "2026-11-02", 1, 7, null, "ALTA", ""), "t"))
                .doesNotThrowAnyException();
    }

    @Test
    void unTipoDeTareaDesconocidoOSinUnidadEsPermanente() {
        assertThatThrownBy(() -> housekeeping.emitir(
                new HousekeepingService.Ticket("PINTAR", "2026-11-02", 1, 7, null, null, null), "t"))
                .isInstanceOf(SolicitudInvalidaException.class)
                .hasMessageContaining("tipoTarea desconocido: PINTAR");
        assertThatThrownBy(() -> housekeeping.emitir(
                new HousekeepingService.Ticket("LIMPIEZA", "2026-11-04", null, 7, null, null, null), "t"))
                .isInstanceOf(SolicitudInvalidaException.class)
                .hasMessageContaining("unitId");
    }

    // ── Voucher ──────────────────────────────────────────────────────────────

    @Test
    void sinTipoDeDocumentoGeneraUnVoucher() {
        assertThatCode(() -> voucher.generar(
                new VoucherService.Solicitud(7, "huesped@andesstay.test", null, DATOS), "t"))
                .doesNotThrowAnyException();
    }

    @Test
    void unDocumentoDesconocidoOSinDestinatarioEsPermanente() {
        assertThatThrownBy(() -> voucher.generar(
                new VoucherService.Solicitud(7, "huesped@andesstay.test", "FACTURA", DATOS), "t"))
                .isInstanceOf(SolicitudInvalidaException.class)
                .hasMessageContaining("tipoDocumento desconocido: FACTURA");
        assertThatThrownBy(() -> voucher.generar(
                new VoucherService.Solicitud(7, null, "VOUCHER", DATOS), "t"))
                .isInstanceOf(SolicitudInvalidaException.class);
    }
}
