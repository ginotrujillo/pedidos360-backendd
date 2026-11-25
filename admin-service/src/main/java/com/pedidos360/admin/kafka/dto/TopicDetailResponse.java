package com.pedidos360.admin.kafka.dto;

import java.util.List;
import java.util.Map;

/** Detalle de un tópico: particiones (líder, réplicas, ISR) y configuración relevante. */
public record TopicDetailResponse(
        String name,
        int partitions,
        int replicationFactor,
        List<PartitionInfo> partitionDetails,
        Map<String, String> configs
) {
    public record PartitionInfo(int partition, int leader, List<Integer> replicas, List<Integer> isr) {
    }
}
