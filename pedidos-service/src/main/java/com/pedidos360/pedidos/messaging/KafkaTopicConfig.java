package com.pedidos360.pedidos.messaging;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declara los tópicos al arrancar (KafkaAdmin crea los que no existen; los que ya existen no se tocan).
 * Es el mismo diseño que crea kafka-init en docker/kafka/docker-compose.kafka.yml.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.kafka.topics.pedidos-eventos}")
    private String topicPedidos;
    @Value("${app.kafka.topics.catalogo-stock}")
    private String topicStock;
    @Value("${app.kafka.topics.pedidos-dlt}")
    private String topicDlt;

    /** Con 2 brokers la replicación es 2. Con un solo broker (pruebas) se puede bajar a 1. */
    @Value("${app.kafka.replicas:2}")
    private int replicas;

    /** Eventos de cambio de estado: 3 particiones (clave = id del pedido), se conservan 7 días. */
    @Bean
    NewTopic pedidosEventosTopic() {
        return TopicBuilder.name(topicPedidos)
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .build();
    }

    /** Stock por producto: compactado, queda el último valor por clave (id del producto). */
    @Bean
    NewTopic catalogoStockTopic() {
        return TopicBuilder.name(topicStock)
                .partitions(2)
                .replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1")
                .config(TopicConfig.SEGMENT_MS_CONFIG, "60000")
                .build();
    }

    /** Dead Letter Topic: aquí van los mensajes que fallan tras los reintentos o que son inválidos. 30 días. */
    @Bean
    NewTopic pedidosDltTopic() {
        return TopicBuilder.name(topicDlt)
                .partitions(3)
                .replicas(replicas)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")
                .build();
    }
}
