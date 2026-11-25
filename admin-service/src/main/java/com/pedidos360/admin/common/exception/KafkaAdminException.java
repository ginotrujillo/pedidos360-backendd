package com.pedidos360.admin.common.exception;

/** Kafka no respondió o devolvió un error inesperado al administrar tópicos o consumer groups. */
public class KafkaAdminException extends RuntimeException {
    public KafkaAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
