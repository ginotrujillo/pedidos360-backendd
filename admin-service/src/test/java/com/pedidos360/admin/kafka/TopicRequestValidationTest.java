package com.pedidos360.admin.kafka;

import com.pedidos360.admin.kafka.dto.TopicRequestDto;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Valida las reglas de Bean Validation del DTO con el que se crean los tópicos. */
class TopicRequestValidationTest {

    private static Validator validator;

    @BeforeAll
    static void crearValidador() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    private Set<ConstraintViolation<TopicRequestDto>> validar(TopicRequestDto dto) {
        return validator.validate(dto);
    }

    @Test
    void requestValidoNoTieneErrores() {
        assertThat(validar(new TopicRequestDto("ofertas.eventos", 3, 2, "compact", 3600000L, 1))).isEmpty();
    }

    @Test
    void nombreVacioOConCaracteresInvalidosEsRechazado() {
        assertThat(validar(new TopicRequestDto("", 3, 2, null, null, null))).isNotEmpty();
        assertThat(validar(new TopicRequestDto("nombre con espacios", 3, 2, null, null, null))).isNotEmpty();
        assertThat(validar(new TopicRequestDto("../etc", 3, 2, null, null, null))).isNotEmpty();
    }

    @Test
    void particionesFueraDeRangoSonRechazadas() {
        assertThat(validar(new TopicRequestDto("t", 0, 2, null, null, null))).isNotEmpty();
        assertThat(validar(new TopicRequestDto("t", 51, 2, null, null, null))).isNotEmpty();
        assertThat(validar(new TopicRequestDto("t", null, 2, null, null, null))).isNotEmpty();
    }

    @Test
    void replicacionFueraDeRangoEsRechazada() {
        assertThat(validar(new TopicRequestDto("t", 3, 0, null, null, null))).isNotEmpty();
        assertThat(validar(new TopicRequestDto("t", 3, 4, null, null, null))).isNotEmpty();
    }

    @Test
    void cleanupPolicyInvalidaEsRechazada() {
        assertThat(validar(new TopicRequestDto("t", 3, 2, "borrar", null, null))).isNotEmpty();
    }

    @Test
    void retencionNegativaOExcesivaEsRechazada() {
        assertThat(validar(new TopicRequestDto("t", 3, 2, null, -1L, null))).isNotEmpty();
        assertThat(validar(new TopicRequestDto("t", 3, 2, null, 2592000001L, null))).isNotEmpty();
    }
}
