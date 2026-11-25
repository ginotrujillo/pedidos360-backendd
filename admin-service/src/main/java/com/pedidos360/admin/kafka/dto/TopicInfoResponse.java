package com.pedidos360.admin.kafka.dto;

/** Resumen de un tópico para el listado. */
public record TopicInfoResponse(String name, int partitions, int replicationFactor) {
}
