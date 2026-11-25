package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.PedidoEventoDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer del dominio AUDITORÍA sobre Kafka.
 * Grupo "auditoria-group": recibe todos los eventos de pedidos.eventos, independiente del grupo de notificaciones.
 */
@Component
public class AuditoriaKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditoriaKafkaConsumer.class);

    @KafkaListener(id = "auditoria-kafka",
            topics = "${app.kafka.topics.pedidos-eventos}",
            groupId = "${app.kafka.groups.auditoria}")
    public void recibir(ConsumerRecord<String, PedidoEventoDto> record) {
        PedidoEventoDto evento = record.value();
        log.info("[KAFKA-AUDITORIA] Recibido topic={} partición={} offset={}",
                record.topic(), record.partition(), record.offset());

        EventoValidator.validarPedido(evento);

        // En una versión más completa se guardaría en una tabla de auditoría.
        log.info("[KAFKA-AUDITORIA] Registrado: pedido={} estado={} total={} timestamp={}",
                evento.pedidoId(), evento.estado(), evento.total(), evento.timestamp());
    }
}
