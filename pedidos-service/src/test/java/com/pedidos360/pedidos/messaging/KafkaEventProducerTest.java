package com.pedidos360.pedidos.messaging;

import com.pedidos360.pedidos.catalog.entity.Product;
import com.pedidos360.pedidos.messaging.dto.PedidoEventoDto;
import com.pedidos360.pedidos.messaging.dto.StockEventoDto;
import com.pedidos360.pedidos.orders.entity.Order;
import com.pedidos360.pedidos.orders.entity.OrderStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventProducerTest {

    private KafkaTemplate<String, Object> template;
    private KafkaEventProducer producer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        template = mock(KafkaTemplate.class);
        when(template.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        producer = new KafkaEventProducer(template, "pedidos.eventos", "catalogo.stock");
    }

    @Test
    void cambioDeEstadoSeEnviaConElIdDelPedidoComoClave() {
        Order o = new Order();
        o.setId(15L);
        o.setStatus(OrderStatus.ACEPTADO);
        o.setCustomerEmail("cliente@correo.cl");
        o.setTotal(new BigDecimal("990.00"));

        producer.publicarCambioEstado(o);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(template).send(eq("pedidos.eventos"), eq("15"), captor.capture());
        assertThat(captor.getValue()).isInstanceOf(PedidoEventoDto.class);
        PedidoEventoDto evento = (PedidoEventoDto) captor.getValue();
        assertThat(evento.pedidoId()).isEqualTo(15L);
        assertThat(evento.estado()).isEqualTo("ACEPTADO");
    }

    @Test
    void stockSeEnviaAlTopicoCompactadoConElIdDelProductoComoClave() {
        Product p = Product.builder().id(3L).name("Mouse").price(new BigDecimal("10.00")).stock(42).build();

        producer.publicarStock(p);

        ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(template).send(eq("catalogo.stock"), eq("3"), captor.capture());
        StockEventoDto evento = (StockEventoDto) captor.getValue();
        assertThat(evento.productoId()).isEqualTo(3L);
        assertThat(evento.stock()).isEqualTo(42);
    }

    @Test
    void siKafkaFallaNoSePropagaElErrorAlNegocio() {
        when(template.send(anyString(), anyString(), any())).thenThrow(new IllegalStateException("broker caído"));
        Order o = new Order();
        o.setId(1L);
        o.setStatus(OrderStatus.CREADO);
        o.setTotal(BigDecimal.TEN);

        assertThatCode(() -> producer.publicarCambioEstado(o)).doesNotThrowAnyException();
    }
}
