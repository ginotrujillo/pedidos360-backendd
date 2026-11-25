package com.pedidos360.admin.kafka.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

/**
 * Datos para crear un tópico.
 *
 * @param name              nombre (letras, números, '.', '-' y '_')
 * @param partitions        cantidad de particiones (1 a 50)
 * @param replicationFactor réplicas por partición (1 a 3; no puede superar la cantidad de brokers)
 * @param cleanupPolicy     (opcional) delete, compact o "compact,delete"
 * @param retentionMs       (opcional) tiempo de retención en milisegundos (máximo 30 días)
 * @param minInsyncReplicas (opcional) réplicas mínimas sincronizadas para aceptar escrituras con acks=all
 */
public record TopicRequestDto(
        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Size(max = 249, message = "El nombre del tópico no puede superar 249 caracteres")
        @Pattern(regexp = "^[a-zA-Z0-9._-]+$",
                message = "El nombre del tópico solo puede tener letras, números, '.', '-' y '_'")
        String name,

        @NotNull(message = "La cantidad de particiones es obligatoria")
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = 50, message = "No se permiten más de 50 particiones")
        Integer partitions,

        @NotNull(message = "El factor de replicación es obligatorio")
        @Min(value = 1, message = "El factor de replicación mínimo es 1")
        @Max(value = 3, message = "El factor de replicación máximo es 3")
        Integer replicationFactor,

        @Pattern(regexp = "^(delete|compact|compact,delete)$",
                message = "cleanupPolicy inválida: use delete, compact o compact,delete")
        String cleanupPolicy,

        @Positive(message = "retentionMs debe ser mayor que 0")
        @Max(value = 2592000000L, message = "retentionMs no puede superar 30 días (2592000000 ms)")
        Long retentionMs,

        @Min(value = 1, message = "minInsyncReplicas mínimo es 1")
        @Max(value = 3, message = "minInsyncReplicas máximo es 3")
        Integer minInsyncReplicas
) {
}
