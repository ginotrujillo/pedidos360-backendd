package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.StockEventoDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer de STOCK sobre Kafka: escucha el tópico compactado catalogo.stock (grupo "stock-group").
 * Avisa cuando el stock de un producto queda bajo el umbral configurado.
 */
@Component
public class StockKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(StockKafkaConsumer.class);

    @Value("${app.kafka.stock-minimo:5}")
    private int stockMinimo;

    @KafkaListener(id = "stock-kafka",
            topics = "${app.kafka.topics.catalogo-stock}",
            groupId = "${app.kafka.groups.stock}",
            concurrency = "2")
    public void recibir(ConsumerRecord<String, StockEventoDto> record) {
        StockEventoDto evento = record.value();
        log.info("[KAFKA-STOCK] Recibido topic={} partición={} offset={} clave={}",
                record.topic(), record.partition(), record.offset(), record.key());

        EventoValidator.validarStock(evento);

        if (evento.stock() <= stockMinimo) {
            log.warn("[KAFKA-STOCK] STOCK BAJO: producto {} ('{}') tiene {} unidades (mínimo {})",
                    evento.productoId(), evento.nombre(), evento.stock(), stockMinimo);
        } else {
            log.info("[KAFKA-STOCK] Producto {} ('{}') con stock {}", evento.productoId(), evento.nombre(), evento.stock());
        }
    }
}
