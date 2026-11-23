package cl.andesstay.notify.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * El controlador de estado no habla con el broker.
 *
 * POR QUE EXISTE. NotifyStatusController inyectaba AmqpAdmin y consultaba las colas el mismo: la capa
 * web conocia la infraestructura de mensajeria. Ahora esa lectura vive en EstadoDeColasService, como
 * el resto de la logica del servicio. Este test es el que impide que la dependencia vuelva: falla en
 * cuanto un campo o un parametro del constructor sea de un tipo de org.springframework.amqp.
 */
class NotifyStatusControllerCapasTest {

    private static final String PAQUETE_AMQP = "org.springframework.amqp";

    @Test
    @DisplayName("NotifyStatusController no depende de ningun tipo de Spring AMQP")
    void sinDependenciasDeAmqp() {
        Class<?> controlador = NotifyStatusController.class;

        Stream<Class<?>> campos = Arrays.stream(controlador.getDeclaredFields()).map(Field::getType);
        Stream<Class<?>> parametros = Arrays.stream(controlador.getDeclaredConstructors())
                .map(Constructor::getParameterTypes)
                .flatMap(Arrays::stream);

        List<String> deAmqp = Stream.concat(campos, parametros)
                .map(Class::getName)
                .filter(nombre -> nombre.startsWith(PAQUETE_AMQP))
                .distinct()
                .toList();

        assertThat(deAmqp).as("tipos de Spring AMQP en el controlador").isEmpty();
    }
}
