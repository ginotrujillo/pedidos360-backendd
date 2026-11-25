package com.pedidos360.admin.rabbit;

import com.pedidos360.admin.common.exception.ResourceNotFoundException;
import com.pedidos360.admin.dto.QueueRequestDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitAdminServiceTest {

    private AmqpAdmin amqpAdmin;
    private RabbitAdminService service;

    @BeforeEach
    void setUp() {
        amqpAdmin = mock(AmqpAdmin.class);
        service = new RabbitAdminService(amqpAdmin, mock(RabbitTemplate.class),
                "pedidos.direct", "pedidos.topic",
                "pedidos.stock.queue", "pedidos.notificaciones.queue", "pedidos.auditoria.queue",
                "pedidos.stock.dlq", "pedidos.notificaciones.dlq", "pedidos.auditoria.dlq");
    }

    private QueueRequestDto cola(String nombre, Boolean conDlq) {
        return new QueueRequestDto(nombre, true, conDlq, null, null);
    }

    @Test
    void crearColaNuevaLaDeclaraEnRabbit() {
        when(amqpAdmin.getQueueInfo("ofertas.queue")).thenReturn(null);

        String creada = service.crearCola(cola("ofertas.queue", false));

        assertThat(creada).isEqualTo("ofertas.queue");
        verify(amqpAdmin).declareQueue(any(Queue.class));
    }

    @Test
    void crearColaConDlqDeclaraAmbasColas() {
        when(amqpAdmin.getQueueInfo("ofertas.queue")).thenReturn(null);
        when(amqpAdmin.getQueueInfo("ofertas.queue.dlq")).thenReturn(null);

        service.crearCola(cola("ofertas.queue", true));

        verify(amqpAdmin, org.mockito.Mockito.times(2)).declareQueue(any(Queue.class));
    }

    @Test
    void crearColaConNombreReservadoLanzaIllegalArgument() {
        assertThatThrownBy(() -> service.crearCola(cola("amq.mi-cola", false)))
                .isInstanceOf(IllegalArgumentException.class);
        verify(amqpAdmin, never()).declareQueue(any(Queue.class));
    }

    @Test
    void crearColaQueYaExisteLanzaIllegalState() {
        when(amqpAdmin.getQueueInfo("ofertas.queue")).thenReturn(new QueueInformation("ofertas.queue", 0, 0));

        assertThatThrownBy(() -> service.crearCola(cola("ofertas.queue", false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ya existe");
    }

    @Test
    void eliminarColaDelSistemaLanzaIllegalState() {
        assertThatThrownBy(() -> service.eliminarCola("pedidos.stock.queue"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Pedidos360");
        verify(amqpAdmin, never()).deleteQueue("pedidos.stock.queue");
    }

    @Test
    void eliminarColaInexistenteLanzaNotFound() {
        when(amqpAdmin.getQueueInfo("fantasma")).thenReturn(null);

        assertThatThrownBy(() -> service.eliminarCola("fantasma"))
                .isInstanceOf(ResourceNotFoundException.class);
    }
}
