package cl.andesstay.notify.email;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import cl.andesstay.notify.service.SolicitudInvalidaException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Las cuatro plantillas que fija infra/docs/contracts/events/rabbit.md, y el caso de prueba que la
 * guía 2.2.3 usa para demostrar la DLQ: una plantilla inexistente es un error permanente y no
 * gasta reintentos.
 */
class EmailTemplatesTest {

    private final EmailTemplates templates = new EmailTemplates();

    private static final Map<String, Object> DATOS = Map.of(
            "nombreHuesped", "Ana Soto",
            "unidad", "Cabana Coigue",
            "fechaEntrada", "2026-10-12",
            "fechaSalida", "2026-10-15",
            "tarifaTotal", 135000);

    @ParameterizedTest
    @ValueSource(strings = {
            "RESERVA_RECIBIDA", "RESERVA_CONFIRMADA", "RECORDATORIO_CHECKIN",
            "CHECKOUT_REALIZADO", "RESERVA_CANCELADA"})
    @DisplayName("todas las plantillas rinden asunto y cuerpo no vacíos")
    void rindeTodasLasPlantillas(String plantilla) {
        EmailTemplates.Correo correo = templates.render(plantilla, "R-2026-000123", DATOS);

        assertFalse(correo.asunto().isBlank(), "el asunto no puede quedar vacío");
        assertFalse(correo.cuerpo().isBlank(), "el cuerpo no puede quedar vacío");
        assertTrue(correo.asunto().contains("AndesStay"));
        assertTrue(correo.cuerpo().contains("Ana Soto"), "el nombre del huésped tiene que aparecer");
    }

    @Test
    @DisplayName("la confirmación incluye unidad, fechas y total")
    void laConfirmacionLlevaLosDatosDeLaReserva() {
        EmailTemplates.Correo correo = templates.render("RESERVA_CONFIRMADA", "R-2026-000123", DATOS);

        assertTrue(correo.cuerpo().contains("Cabana Coigue"));
        assertTrue(correo.cuerpo().contains("2026-10-12"));
        assertTrue(correo.cuerpo().contains("2026-10-15"));
        assertTrue(correo.cuerpo().contains("135000"));
        assertTrue(correo.asunto().contains("R-2026-000123"));
    }

    @Test
    @DisplayName("una plantilla inexistente es error permanente")
    void plantillaInexistenteEsPermanente() {
        SolicitudInvalidaException e = assertThrows(
                SolicitudInvalidaException.class,
                () -> templates.render("PLANTILLA_QUE_NO_EXISTE", "R-1", DATOS));

        assertTrue(e.getMessage().contains("plantilla desconocida"), e.getMessage());
    }

    @Test
    @DisplayName("una plantilla nula es error permanente")
    void plantillaNulaEsPermanente() {
        assertThrows(SolicitudInvalidaException.class,
                () -> templates.render(null, "R-1", DATOS));
    }

    @Test
    @DisplayName("sin datos ni reservationId el render no estalla")
    void toleraDatosIncompletos() {
        // Un payload incompleto es un problema del publicador, pero el correo tiene que salir con
        // valores por omisión en lugar de convertirse en un NullPointerException.
        EmailTemplates.Correo correo = templates.render("RESERVA_CANCELADA", null, Map.of());

        assertFalse(correo.cuerpo().isBlank());
        assertTrue(correo.cuerpo().contains("huésped"));
    }
}
