package com.pedidos360.eventos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Monitor del Dead Letter Topic: registra en el log cada mensaje que no pudo procesarse,
 * con su tópico de origen y el motivo del fallo. Lee el valor como texto plano.
 */
@Component
public class DltKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(DltKafkaConsumer.class);

    @KafkaListener(id = "dlt-kafka",
            topics = "${app.kafka.topics.pedidos-dlt}",
            groupId = "${app.kafka.groups.dlt}",
            concurrency = "1",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    public void recibir(ConsumerRecord<String, String> record) {
        log.error("[KAFKA-DLT] Mensaje fallido. origen={} clave={} motivo='{}' contenido={}",
                cabecera(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                record.key(),
                cabecera(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
                record.value());
    }

    static String cabecera(ConsumerRecord<?, ?> record, String nombre) {
        Header header = record.headers().lastHeader(nombre);
        return header == null || header.value() == null
                ? "desconocido"
                : new String(header.value(), StandardCharsets.UTF_8);
    }
}
