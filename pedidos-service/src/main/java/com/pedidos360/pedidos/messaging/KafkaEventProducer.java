package com.pedidos360.pedidos.messaging;

import com.pedidos360.pedidos.catalog.entity.Product;
import com.pedidos360.pedidos.messaging.dto.PedidoEventoDto;
import com.pedidos360.pedidos.messaging.dto.StockEventoDto;
import com.pedidos360.pedidos.orders.entity.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * Productor Kafka de Pedidos360.
 *  - pedidos.eventos : un evento por cada cambio de estado de un pedido (clave = id del pedido,
 *                      así todos los eventos de un pedido caen en la misma partición y mantienen el orden).
 *  - catalogo.stock  : stock vigente de un producto (clave = id del producto, tópico compactado).
 *
 * El envío es asíncrono y nunca rompe la operación de negocio: si Kafka no responde, se registra el error.
 */
@Component
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topicPedidos;
    private final String topicStock;

    public KafkaEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                              @Value("${app.kafka.topics.pedidos-eventos}") String topicPedidos,
                              @Value("${app.kafka.topics.catalogo-stock}") String topicStock) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicPedidos = topicPedidos;
        this.topicStock = topicStock;
    }

    public void publicarCambioEstado(Order order) {
        PedidoEventoDto evento = new PedidoEventoDto(
                order.getId(), order.getStatus().name(), order.getCustomerEmail(),
                order.getTotal(), LocalDateTime.now());
        enviar(topicPedidos, String.valueOf(order.getId()), evento);
    }

    public void publicarStock(Product product) {
        StockEventoDto evento = new StockEventoDto(
                product.getId(), product.getName(), product.getStock(), LocalDateTime.now());
        enviar(topicStock, String.valueOf(product.getId()), evento);
    }

    private void enviar(String topic, String clave, Object evento) {
        try {
            kafkaTemplate.send(topic, clave, evento).whenComplete((resultado, error) -> {
                if (error != null) {
                    log.error("[KAFKA] No se pudo publicar en '{}' (clave={}): {}", topic, clave, error.getMessage());
                } else {
                    log.info("[KAFKA] Publicado en '{}' clave={} partición={} offset={}", topic, clave,
                            resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
                }
            });
        } catch (Exception e) {
            log.error("[KAFKA] Error enviando a '{}' (clave={}): {}", topic, clave, e.getMessage());
        }
    }
}
