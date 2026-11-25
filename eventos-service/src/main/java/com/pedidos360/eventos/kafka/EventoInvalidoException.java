package com.pedidos360.eventos.kafka;

/**
 * El mensaje llegó bien pero su contenido no es válido (faltan campos, valores imposibles).
 * Reintentar no sirve de nada, así que el manejador de errores NO la reintenta y la envía directo al DLT.
 */
public class EventoInvalidoException extends RuntimeException {
    public EventoInvalidoException(String message) {
        super(message);
    }
}
