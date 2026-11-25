package com.pedidos360.eventos.rabbit;

import com.pedidos360.eventos.rabbit.dto.PedidoEventoDto;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RabbitConsumersTest {

    private static final long TAG = 42L;

    private Channel channel;

    @BeforeEach
    void setUp() {
        channel = mock(Channel.class);
    }

    private PedidoEventoDto evento(Long id, String email, BigDecimal total) {
        return new PedidoEventoDto(id, "ACEPTADO", email, total, LocalDateTime.now());
    }

    // ---------- Stock ----------
    @Test
    void stockConEventoValidoHaceAck() throws Exception {
        StockEventConsumer consumer = new StockEventConsumer();
        ReflectionTestUtils.setField(consumer, "dlq", "pedidos.stock.dlq");

        consumer.recibir(evento(1L, "a@b.cl", new BigDecimal("100")), channel, TAG);

        verify(channel).basicAck(TAG, false);
        verify(channel, never()).basicNack(TAG, false, false);
    }

    @Test
    void stockConTotalCeroHaceNackSinReencolar() throws Exception {
        StockEventConsumer consumer = new StockEventConsumer();
        ReflectionTestUtils.setField(consumer, "dlq", "pedidos.stock.dlq");

        consumer.recibir(evento(1L, "a@b.cl", BigDecimal.ZERO), channel, TAG);

        verify(channel).basicNack(TAG, false, false);
        verify(channel, never()).basicAck(TAG, false);
    }

    // ---------- Notificaciones ----------
    @Test
    void notificacionConEmailHaceAck() throws Exception {
        NotificacionEventConsumer consumer = new NotificacionEventConsumer();
        ReflectionTestUtils.setField(consumer, "dlq", "pedidos.notificaciones.dlq");

        consumer.recibir(evento(1L, "a@b.cl", BigDecimal.TEN), channel, TAG);

        verify(channel).basicAck(TAG, false);
    }

    @Test
    void notificacionSinEmailHaceNackSinReencolar() throws Exception {
        NotificacionEventConsumer consumer = new NotificacionEventConsumer();
        ReflectionTestUtils.setField(consumer, "dlq", "pedidos.notificaciones.dlq");

        consumer.recibir(evento(1L, " ", BigDecimal.TEN), channel, TAG);

        verify(channel).basicNack(TAG, false, false);
        verify(channel, never()).basicAck(TAG, false);
    }

    // ---------- Auditoría ----------
    @Test
    void auditoriaConPedidoIdHaceAck() throws Exception {
        AuditoriaEventConsumer consumer = new AuditoriaEventConsumer();
        ReflectionTestUtils.setField(consumer, "dlq", "pedidos.auditoria.dlq");

        consumer.recibir(evento(5L, null, BigDecimal.TEN), channel, TAG);

        verify(channel).basicAck(TAG, false);
    }

    @Test
    void auditoriaSinPedidoIdHaceNackSinReencolar() throws Exception {
        AuditoriaEventConsumer consumer = new AuditoriaEventConsumer();
        ReflectionTestUtils.setField(consumer, "dlq", "pedidos.auditoria.dlq");

        consumer.recibir(evento(null, "a@b.cl", BigDecimal.TEN), channel, TAG);

        verify(channel).basicNack(TAG, false, false);
    }
}
