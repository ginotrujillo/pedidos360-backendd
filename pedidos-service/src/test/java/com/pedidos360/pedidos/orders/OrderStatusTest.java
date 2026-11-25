package com.pedidos360.pedidos.orders;

import com.pedidos360.pedidos.orders.entity.OrderStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStatusTest {

    @Test
    void creadoPuedeIrAAceptadoOCancelado() {
        assertThat(OrderStatus.CREADO.canTransitionTo(OrderStatus.ACEPTADO)).isTrue();
        assertThat(OrderStatus.CREADO.canTransitionTo(OrderStatus.CANCELADO)).isTrue();
    }

    @Test
    void creadoNoPuedeSaltarADespachado() {
        assertThat(OrderStatus.CREADO.canTransitionTo(OrderStatus.DESPACHADO)).isFalse();
    }

    @Test
    void flujoNormalEsValido() {
        assertThat(OrderStatus.ACEPTADO.canTransitionTo(OrderStatus.EN_PREPARACION)).isTrue();
        assertThat(OrderStatus.EN_PREPARACION.canTransitionTo(OrderStatus.DESPACHADO)).isTrue();
        assertThat(OrderStatus.DESPACHADO.canTransitionTo(OrderStatus.ENTREGADO)).isTrue();
    }

    @Test
    void estadosFinalesNoTienenTransiciones() {
        for (OrderStatus destino : OrderStatus.values()) {
            assertThat(OrderStatus.ENTREGADO.canTransitionTo(destino)).isFalse();
            assertThat(OrderStatus.CANCELADO.canTransitionTo(destino)).isFalse();
        }
    }
}
