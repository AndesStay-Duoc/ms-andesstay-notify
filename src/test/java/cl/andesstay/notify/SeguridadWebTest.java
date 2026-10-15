package cl.andesstay.notify;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.RequestPostProcessor;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Superficie HTTP de notify: quién puede llamar a qué.
 *
 * Contexto completo y no una rebanada @WebMvcTest, porque /actuator/metrics y /actuator/health
 * solo existen con Actuator cargado. Para no necesitar un broker: los listeners no arrancan,
 * AmqpAdmin es un mock y los indicadores de salud de RabbitMQ y de correo se apagan solo aquí.
 */
@SpringBootTest(properties = {
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "management.health.rabbit.enabled=false",
        "management.health.mail.enabled=false"
})
@AutoConfigureMockMvc
class SeguridadWebTest {

    private static final String ESTADO = "/api/v1/notify/status";
    private static final String METRICAS = "/actuator/metrics";

    @Autowired
    private MockMvc mvc;
    @MockBean
    private AmqpAdmin amqpAdmin;

    private static RequestPostProcessor conRol(String rol) {
        return jwt().authorities(new SimpleGrantedAuthority("ROLE_" + rol));
    }

    // ── Health: abierto, lo usa el healthcheck del contenedor ───────────────

    @Test
    void elHealthNoExigeToken() throws Exception {
        mvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    // ── GET /api/v1/notify/status ────────────────────────────────────────────

    @Test
    void elEstadoSinTokenResponde401() throws Exception {
        mvc.perform(get(ESTADO)).andExpect(status().isUnauthorized());
    }

    @Test
    void elEstadoConUnRolDistintoDeAdminResponde403() throws Exception {
        mvc.perform(get(ESTADO).with(conRol("Operador")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("ROL_INSUFICIENTE"));
    }

    @Test
    void elEstadoConAdminResponde200() throws Exception {
        // AmqpAdmin es un mock que no conoce ninguna cola: el endpoint lo dice en lugar de fallar.
        mvc.perform(get(ESTADO).with(conRol("Admin")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.servicio").value("ms-andesstay-notify"))
                .andExpect(jsonPath("$.flujos.email.trabajo['q.cmd.email']").value("no existe en el broker"));
    }

    // ── /actuator/metrics ────────────────────────────────────────────────────

    @Test
    void lasMetricasSinTokenResponden401() throws Exception {
        mvc.perform(get(METRICAS)).andExpect(status().isUnauthorized());
        mvc.perform(get(METRICAS + "/notify.messages.dlq")).andExpect(status().isUnauthorized());
    }

    @Test
    void lasMetricasConUnRolDistintoDeAdminResponden403() throws Exception {
        mvc.perform(get(METRICAS).with(conRol("Auditor"))).andExpect(status().isForbidden());
    }

    @Test
    void lasMetricasConAdminResponden200() throws Exception {
        mvc.perform(get(METRICAS).with(conRol("Admin")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.names").isArray());
    }
}
