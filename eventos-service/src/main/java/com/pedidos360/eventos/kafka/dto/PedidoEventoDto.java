package com.pedidos360.eventos.kafka.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** Evento del tópico pedidos.eventos (contrato propio de este servicio, independiente de pedidos-service). */
public record PedidoEventoDto(
        Long pedidoId,
        String estado,
        String clienteEmail,
        BigDecimal total,
        LocalDateTime timestamp
) {
}
