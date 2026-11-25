package com.pedidos360.pedidos.messaging;

import com.pedidos360.pedidos.messaging.dto.PedidoEventoDto;
import com.pedidos360.pedidos.orders.entity.Order;
import com.pedidos360.pedidos.orders.entity.OrderStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class PedidoEventPublisherTest {

    private RabbitTemplate rabbitTemplate;
    private PedidoEventPublisher publisher;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        publisher = new PedidoEventPublisher(rabbitTemplate);
        ReflectionTestUtils.setField(publisher, "directExchange", "pedidos.direct");
        ReflectionTestUtils.setField(publisher, "topicExchange", "pedidos.topic");
    }

    private Order pedido(OrderStatus estado) {
        Order o = new Order();
        o.setId(7L);
        o.setStatus(estado);
        o.setCustomerEmail("cliente@correo.cl");
        o.setTotal(new BigDecimal("250.00"));
        return o;
    }

    @Test
    void estadoCreadoSoloVaAlExchangeTopic() {
        publisher.publicarCambioEstado(pedido(OrderStatus.CREADO));

        verify(rabbitTemplate).convertAndSend(eq("pedidos.topic"), eq("pedido.creado.evento"), any(PedidoEventoDto.class));
        verify(rabbitTemplate, never()).convertAndSend(eq("pedidos.direct"), any(String.class), any(PedidoEventoDto.class));
    }

    @Test
    void estadoAceptadoVaTambienAlExchangeDirect() {
        publisher.publicarCambioEstado(pedido(OrderStatus.ACEPTADO));

        verify(rabbitTemplate).convertAndSend(eq("pedidos.topic"), eq("pedido.aceptado.evento"), any(PedidoEventoDto.class));
        verify(rabbitTemplate).convertAndSend(eq("pedidos.direct"), eq("pedido.aceptado.evento"), any(PedidoEventoDto.class));
    }

    @Test
    void elEventoLlevaLosDatosDelPedido() {
        publisher.publicarCambioEstado(pedido(OrderStatus.EN_PREPARACION));

        ArgumentCaptor<PedidoEventoDto> captor = ArgumentCaptor.forClass(PedidoEventoDto.class);
        verify(rabbitTemplate).convertAndSend(eq("pedidos.topic"), eq("pedido.en_preparacion.evento"), captor.capture());
        PedidoEventoDto evento = captor.getValue();
        assertThat(evento.pedidoId()).isEqualTo(7L);
        assertThat(evento.estado()).isEqualTo("EN_PREPARACION");
        assertThat(evento.clienteEmail()).isEqualTo("cliente@correo.cl");
        assertThat(evento.total()).isEqualByComparingTo("250.00");
    }
}
