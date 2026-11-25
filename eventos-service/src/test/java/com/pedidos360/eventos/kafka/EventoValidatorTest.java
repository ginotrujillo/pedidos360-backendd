package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.PedidoEventoDto;
import com.pedidos360.eventos.kafka.dto.StockEventoDto;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventoValidatorTest {

    private PedidoEventoDto pedido(Long id, String estado, String email) {
        return new PedidoEventoDto(id, estado, email, new BigDecimal("100.00"), LocalDateTime.now());
    }

    @Test
    void pedidoValidoPasa() {
        assertThatCode(() -> EventoValidator.validarPedido(pedido(1L, "CREADO", null))).doesNotThrowAnyException();
        assertThatCode(() -> EventoValidator.validarPedidoConEmail(pedido(1L, "CREADO", "a@b.cl")))
                .doesNotThrowAnyException();
    }

    @Test
    void eventoNuloEsInvalido() {
        assertThatThrownBy(() -> EventoValidator.validarPedido(null)).isInstanceOf(EventoInvalidoException.class);
    }

    @Test
    void pedidoSinIdEsInvalido() {
        assertThatThrownBy(() -> EventoValidator.validarPedido(pedido(null, "CREADO", "a@b.cl")))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("pedidoId");
    }

    @Test
    void pedidoSinEstadoEsInvalido() {
        assertThatThrownBy(() -> EventoValidator.validarPedido(pedido(1L, " ", "a@b.cl")))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("estado");
    }

    @Test
    void notificacionSinEmailEsInvalida() {
        assertThatThrownBy(() -> EventoValidator.validarPedidoConEmail(pedido(1L, "CREADO", "")))
                .isInstanceOf(EventoInvalidoException.class)
                .hasMessageContaining("email");
    }

    @Test
    void stockValidoPasaYNegativoFalla() {
        assertThatCode(() -> EventoValidator.validarStock(new StockEventoDto(1L, "Mouse", 0, LocalDateTime.now())))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> EventoValidator.validarStock(new StockEventoDto(1L, "Mouse", -3, LocalDateTime.now())))
                .isInstanceOf(EventoInvalidoException.class);
        assertThatThrownBy(() -> EventoValidator.validarStock(new StockEventoDto(null, "Mouse", 3, LocalDateTime.now())))
                .isInstanceOf(EventoInvalidoException.class);
    }
}
