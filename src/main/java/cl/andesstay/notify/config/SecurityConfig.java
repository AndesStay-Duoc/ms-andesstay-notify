package cl.andesstay.notify.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;

/**
 * La misma configuración que ms-andesstay-audit y ms-andesstay-report: JWT de Azure AD, el claim
 * "roles" convertido a ROLE_*, y la autorización por rol con @PreAuthorize en el controlador.
 *
 * La única diferencia es la regla de /actuator/metrics: @PreAuthorize no se puede poner sobre los
 * endpoints de Actuator, así que su rol se exige aquí, por ruta. /actuator/health sigue abierto
 * porque lo usa el healthcheck del contenedor.
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(c -> c.disable())
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(a -> a.requestMatchers("/actuator/health").permitAll()
                // Única diferencia con audit y report: las métricas de la DLQ, solo para Admin.
                .requestMatchers("/actuator/metrics", "/actuator/metrics/**").hasRole("Admin")
                .anyRequest().authenticated())
            .oauth2ResourceServer(o -> o.jwt(j -> j.jwtAuthenticationConverter(jwtConverter())));
        return http.build();
    }

    @Bean
    public JwtAuthenticationConverter jwtConverter() {
        JwtGrantedAuthoritiesConverter gc = new JwtGrantedAuthoritiesConverter();
        gc.setAuthoritiesClaimName("roles");
        gc.setAuthorityPrefix("ROLE_");
        JwtAuthenticationConverter c = new JwtAuthenticationConverter();
        c.setJwtGrantedAuthoritiesConverter(gc);
        return c;
    }
}
