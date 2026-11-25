package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.PedidoEventoDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumer del dominio NOTIFICACIONES sobre Kafka.
 * Grupo "notificaciones-group": recibe TODOS los eventos de pedidos.eventos (cada grupo tiene su propia copia).
 * Si se levantan varias instancias de este servicio, las 3 particiones se reparten entre ellas.
 *
 * Errores: si el contenido es inválido lanza EventoInvalidoException (sin reintentos, va directo al DLT);
 * cualquier otro error se reintenta y, agotados los reintentos, también va al DLT (ver KafkaConsumerConfig).
 */
@Component
public class NotificacionKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificacionKafkaConsumer.class);

    /** Solo para demostrar reintentos: simula que el servicio de correo está caído. */
    @Value("${app.kafka.demo.fallar-notificaciones:false}")
    private boolean simularFallo;

    @KafkaListener(id = "notificaciones-kafka",
            topics = "${app.kafka.topics.pedidos-eventos}",
            groupId = "${app.kafka.groups.notificaciones}")
    public void recibir(ConsumerRecord<String, PedidoEventoDto> record) {
        PedidoEventoDto evento = record.value();
        log.info("[KAFKA-NOTIFICACIONES] Recibido topic={} partición={} offset={} clave={}",
                record.topic(), record.partition(), record.offset(), record.key());

        EventoValidator.validarPedidoConEmail(evento);

        if (simularFallo) {
            throw new IllegalStateException("Servicio de correo no disponible (fallo simulado)");
        }

        log.info("[KAFKA-NOTIFICACIONES] Enviado aviso a {} -> pedido {} ahora está {}",
                evento.clienteEmail(), evento.pedidoId(), evento.estado());
    }
}
