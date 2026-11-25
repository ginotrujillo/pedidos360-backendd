package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.PedidoEventoDto;
import com.pedidos360.eventos.kafka.dto.StockEventoDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Manejo de errores de los consumers Kafka:
 *
 *   1. Mensaje que no se puede deserializar (JSON roto)  -> ErrorHandlingDeserializer, va directo al DLT.
 *   2. Contenido inválido (EventoInvalidoException)      -> sin reintentos, va directo al DLT.
 *   3. Cualquier otro error (p. ej. servicio caído)      -> N reintentos con espera fija; si siguen
 *                                                          fallando, el mensaje va al DLT.
 *
 * Cada reintento y cada envío al DLT quedan registrados en el log.
 * Spring Boot aplica este DefaultErrorHandler a todos los @KafkaListener.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    /** Productor propio, usado solo para publicar en el DLT. Sabe enviar bytes (JSON roto) y los DTO. */
    @Bean
    public ProducerFactory<String, Object> dltProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        Map<Class<?>, Serializer<?>> serializadores = new HashMap<>();
        serializadores.put(byte[].class, new ByteArraySerializer());
        serializadores.put(PedidoEventoDto.class, new JacksonJsonSerializer<PedidoEventoDto>());
        serializadores.put(StockEventoDto.class, new JacksonJsonSerializer<StockEventoDto>());

        return new DefaultKafkaProducerFactory<String, Object>(
                config, new StringSerializer(), new DelegatingByTypeSerializer(serializadores));
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate(ProducerFactory<String, Object> dltProducerFactory) {
        return new KafkaTemplate<String, Object>(dltProducerFactory);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, Object> dltKafkaTemplate,
            @Value("${app.kafka.topics.pedidos-dlt}") String topicDlt,
            @Value("${app.kafka.retry.intervalo-ms:1000}") long intervaloMs,
            @Value("${app.kafka.retry.max-reintentos:3}") long maxReintentos) {

        // Partición -1: Kafka elige la partición del DLT según la clave del mensaje
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltKafkaTemplate, (record, ex) -> new TopicPartition(topicDlt, -1));

        DefaultErrorHandler handler = new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception ex) -> {
                    log.error("[KAFKA-ERROR] Agotados los reintentos o error no reintentable. "
                                    + "topic={} partición={} offset={} -> se envía al DLT '{}'. Motivo: {}",
                            record.topic(), record.partition(), record.offset(), topicDlt, causa(ex));
                    recoverer.accept(record, ex);
                },
                new FixedBackOff(intervaloMs, maxReintentos));

        // El contenido inválido no mejora reintentando: directo al DLT
        handler.addNotRetryableExceptions(EventoInvalidoException.class);

        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                log.warn("[KAFKA-REINTENTO] Intento {} falló. topic={} partición={} offset={}. Motivo: {}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), causa(ex));
            }
        });
        return handler;
    }

    private static String causa(Exception ex) {
        Throwable t = ex.getCause() != null ? ex.getCause() : ex;
        return t.getClass().getSimpleName() + ": " + t.getMessage();
    }
}
