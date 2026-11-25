package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.PedidoEventoDto;
import com.pedidos360.eventos.kafka.dto.StockEventoDto;

/** Validaciones de contenido de los eventos recibidos por Kafka. Lanzan EventoInvalidoException. */
public final class EventoValidator {

    private EventoValidator() {
    }

    /** Validación común a todos los consumidores de pedidos.eventos. */
    public static void validarPedido(PedidoEventoDto evento) {
        if (evento == null) {
            throw new EventoInvalidoException("El evento de pedido llegó vacío");
        }
        if (evento.pedidoId() == null) {
            throw new EventoInvalidoException("El evento no trae pedidoId");
        }
        if (evento.estado() == null || evento.estado().isBlank()) {
            throw new EventoInvalidoException("El evento del pedido " + evento.pedidoId() + " no trae estado");
        }
    }

    public static void validarPedidoConEmail(PedidoEventoDto evento) {
        validarPedido(evento);
        if (evento.clienteEmail() == null || evento.clienteEmail().isBlank()) {
            throw new EventoInvalidoException(
                    "El evento del pedido " + evento.pedidoId() + " no trae email del cliente");
        }
    }

    public static void validarStock(StockEventoDto evento) {
        if (evento == null) {
            throw new EventoInvalidoException("El evento de stock llegó vacío");
        }
        if (evento.productoId() == null) {
            throw new EventoInvalidoException("El evento de stock no trae productoId");
        }
        if (evento.stock() == null || evento.stock() < 0) {
            throw new EventoInvalidoException(
                    "Stock inválido para el producto " + evento.productoId() + ": " + evento.stock());
        }
    }
}
