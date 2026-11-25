package com.pedidos360.eventos.kafka.dto;

import java.time.LocalDateTime;

/** Evento del tópico compactado catalogo.stock. */
public record StockEventoDto(
        Long productoId,
        String nombre,
        Integer stock,
        LocalDateTime timestamp
) {
}
