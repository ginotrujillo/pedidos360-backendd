package com.pedidos360.admin.kafka.dto;

import java.util.List;

/**
 * Detalle de un consumer group: miembros con sus particiones asignadas y el lag por partición
 * (lag = último offset del tópico menos el offset confirmado por el grupo).
 */
public record ConsumerGroupDetail(
        String groupId,
        String state,
        List<MemberInfo> members,
        List<OffsetInfo> offsets,
        long totalLag
) {
    public record MemberInfo(String clientId, String host, List<String> partitions) {
    }

    public record OffsetInfo(String topic, int partition, long currentOffset, long endOffset, long lag) {
    }
}
