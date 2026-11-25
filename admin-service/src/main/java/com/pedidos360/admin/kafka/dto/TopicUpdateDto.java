package com.pedidos360.admin.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/**
 * Datos para modificar un tópico existente. Todos los campos son opcionales, pero debe venir al menos uno.
 * Las particiones solo se pueden aumentar, nunca reducir (límite de Kafka).
 */
public record TopicUpdateDto(
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = 50, message = "No se permiten más de 50 particiones")
        Integer partitions,

        @Pattern(regexp = "^(delete|compact|compact,delete)$",
                message = "cleanupPolicy inválida: use delete, compact o compact,delete")
        String cleanupPolicy,

        @Positive(message = "retentionMs debe ser mayor que 0")
        @Max(value = 2592000000L, message = "retentionMs no puede superar 30 días (2592000000 ms)")
        Long retentionMs
) {
}
