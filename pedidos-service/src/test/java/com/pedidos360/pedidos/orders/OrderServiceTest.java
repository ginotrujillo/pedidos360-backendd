package com.pedidos360.pedidos.orders;

import com.pedidos360.pedidos.catalog.entity.Product;
import com.pedidos360.pedidos.catalog.repository.ProductRepository;
import com.pedidos360.pedidos.common.exception.ResourceNotFoundException;
import com.pedidos360.pedidos.messaging.KafkaEventProducer;
import com.pedidos360.pedidos.messaging.PedidoEventPublisher;
import com.pedidos360.pedidos.orders.dto.CreateOrderRequest;
import com.pedidos360.pedidos.orders.dto.OrderResponse;
import com.pedidos360.pedidos.orders.entity.Order;
import com.pedidos360.pedidos.orders.entity.OrderItem;
import com.pedidos360.pedidos.orders.entity.OrderStatus;
import com.pedidos360.pedidos.orders.repository.OrderRepository;
import com.pedidos360.pedidos.orders.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrderServiceTest {

    private OrderRepository orders;
    private ProductRepository products;
    private PedidoEventPublisher rabbitPublisher;
    private KafkaEventProducer kafkaProducer;
    private OrderService service;

    @BeforeEach
    void setUp() {
        orders = mock(OrderRepository.class);
        products = mock(ProductRepository.class);
        rabbitPublisher = mock(PedidoEventPublisher.class);
        kafkaProducer = mock(KafkaEventProducer.class);
        service = new OrderService(orders, products, rabbitPublisher, kafkaProducer);
    }

    private Product producto(long id, int stock) {
        return Product.builder().id(id).name("Teclado").price(new BigDecimal("100.00")).stock(stock).build();
    }

    private Order pedido(long id, OrderStatus estado, Product p, int cantidad) {
        Order o = new Order();
        o.setId(id);
        o.setStatus(estado);
        o.setCustomerEmail("cliente@correo.cl");
        o.setTotal(p.getPrice().multiply(BigDecimal.valueOf(cantidad)));
        OrderItem item = new OrderItem();
        item.setOrder(o);
        item.setProduct(p);
        item.setQuantity(cantidad);
        item.setUnitPrice(p.getPrice());
        o.getItems().add(item);
        return o;
    }

    @Test
    void crearPedidoCalculaTotalYPublicaEnRabbitYKafka() {
        Product p = producto(1L, 10);
        when(products.findById(1L)).thenReturn(Optional.of(p));
        when(orders.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));
        Jwt jwt = Jwt.withTokenValue("t").header("alg", "none")
                .claim("oid", "user-1").claim("name", "Cliente Uno")
                .claim("preferred_username", "cliente@correo.cl").build();

        OrderResponse resp = service.create(
                new CreateOrderRequest(List.of(new CreateOrderRequest.Item(1L, 3))), jwt);

        assertThat(resp.total()).isEqualByComparingTo("300.00");
        assertThat(resp.status()).isEqualTo(OrderStatus.CREADO);
        verify(rabbitPublisher).publicarCambioEstado(any(Order.class));
        verify(kafkaProducer).publicarCambioEstado(any(Order.class));
    }

    @Test
    void crearPedidoConProductoInexistenteLanzaNotFound() {
        when(products.findById(99L)).thenReturn(Optional.empty());
        Jwt jwt = Jwt.withTokenValue("t").header("alg", "none").claim("oid", "user-1").build();

        assertThatThrownBy(() -> service.create(
                new CreateOrderRequest(List.of(new CreateOrderRequest.Item(99L, 1))), jwt))
                .isInstanceOf(ResourceNotFoundException.class);
        verify(kafkaProducer, never()).publicarCambioEstado(any(Order.class));
    }

    @Test
    void aceptarPedidoDescuentaStockYPublicaEventos() {
        Product p = producto(1L, 5);
        Order o = pedido(10L, OrderStatus.CREADO, p, 2);
        when(orders.findById(10L)).thenReturn(Optional.of(o));

        service.updateStatus(10L, OrderStatus.ACEPTADO);

        assertThat(p.getStock()).isEqualTo(3);
        assertThat(o.getStatus()).isEqualTo(OrderStatus.ACEPTADO);
        verify(rabbitPublisher).publicarCambioEstado(o);
        verify(kafkaProducer).publicarCambioEstado(o);
        verify(kafkaProducer).publicarStock(p);
    }

    @Test
    void aceptarConStockInsuficienteLanzaIllegalState() {
        Product p = producto(1L, 1);
        Order o = pedido(10L, OrderStatus.CREADO, p, 2);
        when(orders.findById(10L)).thenReturn(Optional.of(o));

        assertThatThrownBy(() -> service.updateStatus(10L, OrderStatus.ACEPTADO))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stock insuficiente");
        assertThat(p.getStock()).isEqualTo(1);
        verify(kafkaProducer, never()).publicarCambioEstado(any(Order.class));
    }

    @Test
    void noSePuedeDespacharUnPedidoNoAceptado() {
        Product p = producto(1L, 5);
        Order o = pedido(10L, OrderStatus.CREADO, p, 1);
        when(orders.findById(10L)).thenReturn(Optional.of(o));

        assertThatThrownBy(() -> service.updateStatus(10L, OrderStatus.DESPACHADO))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no fue aceptado");
    }

    @Test
    void cancelarPedidoAceptadoRestauraStock() {
        Product p = producto(1L, 3);
        Order o = pedido(10L, OrderStatus.ACEPTADO, p, 2);
        when(orders.findById(10L)).thenReturn(Optional.of(o));

        service.updateStatus(10L, OrderStatus.CANCELADO);

        assertThat(p.getStock()).isEqualTo(5);
        verify(kafkaProducer).publicarStock(p);
    }

    @Test
    void cancelarPedidoSinAceptarNoTocaElStock() {
        Product p = producto(1L, 3);
        Order o = pedido(10L, OrderStatus.CREADO, p, 2);
        when(orders.findById(10L)).thenReturn(Optional.of(o));

        service.updateStatus(10L, OrderStatus.CANCELADO);

        assertThat(p.getStock()).isEqualTo(3);
        verify(kafkaProducer, never()).publicarStock(any(Product.class));
    }

    @Test
    void cambiarEstadoDePedidoInexistenteLanzaNotFound() {
        when(orders.findById(404L)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.updateStatus(404L, OrderStatus.ACEPTADO))
                .isInstanceOf(ResourceNotFoundException.class);
    }
}
