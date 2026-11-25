package com.pedidos360.pedidos.messaging.dto;

import java.time.LocalDateTime;

/** Evento Kafka del tópico compactado catalogo.stock: el último valor por producto es el stock vigente. */
public record StockEventoDto(
        Long productoId,
        String nombre,
        Integer stock,
        LocalDateTime timestamp
) {
}
