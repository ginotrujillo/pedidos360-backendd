package com.pedidos360.eventos.kafka;

import com.pedidos360.eventos.kafka.dto.PedidoEventoDto;
import com.pedidos360.eventos.kafka.dto.StockEventoDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaConsumersTest {

    private ConsumerRecord<String, PedidoEventoDto> registroPedido(PedidoEventoDto evento) {
        return new ConsumerRecord<>("pedidos.eventos", 0, 0L, "1", evento);
    }

    private PedidoEventoDto pedido(Long id, String email) {
        return new PedidoEventoDto(id, "ACEPTADO", email, new BigDecimal("500.00"), LocalDateTime.now());
    }

    @Test
    void notificacionProcesaEventoValido() {
        NotificacionKafkaConsumer consumer = new NotificacionKafkaConsumer();
        assertThatCode(() -> consumer.recibir(registroPedido(pedido(1L, "a@b.cl")))).doesNotThrowAnyException();
    }

    @Test
    void notificacionSinEmailLanzaEventoInvalido() {
        NotificacionKafkaConsumer consumer = new NotificacionKafkaConsumer();
        assertThatThrownBy(() -> consumer.recibir(registroPedido(pedido(1L, null))))
                .isInstanceOf(EventoInvalidoException.class);
    }

    @Test
    void notificacionConFalloSimuladoLanzaErrorReintentable() {
        NotificacionKafkaConsumer consumer = new NotificacionKafkaConsumer();
        ReflectionTestUtils.setField(consumer, "simularFallo", true);

        assertThatThrownBy(() -> consumer.recibir(registroPedido(pedido(1L, "a@b.cl"))))
                .isInstanceOf(IllegalStateException.class)
                .isNotInstanceOf(EventoInvalidoException.class);
    }

    @Test
    void auditoriaProcesaEventoValidoYRechazaElInvalido() {
        AuditoriaKafkaConsumer consumer = new AuditoriaKafkaConsumer();
        assertThatCode(() -> consumer.recibir(registroPedido(pedido(2L, null)))).doesNotThrowAnyException();
        assertThatThrownBy(() -> consumer.recibir(registroPedido(pedido(null, "a@b.cl"))))
                .isInstanceOf(EventoInvalidoException.class);
    }

    @Test
    void stockProcesaStockBajoYNormal() {
        StockKafkaConsumer consumer = new StockKafkaConsumer();
        ReflectionTestUtils.setField(consumer, "stockMinimo", 5);

        ConsumerRecord<String, StockEventoDto> bajo = new ConsumerRecord<>("catalogo.stock", 0, 0L, "9",
                new StockEventoDto(9L, "Mouse", 2, LocalDateTime.now()));
        ConsumerRecord<String, StockEventoDto> normal = new ConsumerRecord<>("catalogo.stock", 0, 1L, "9",
                new StockEventoDto(9L, "Mouse", 50, LocalDateTime.now()));

        assertThatCode(() -> consumer.recibir(bajo)).doesNotThrowAnyException();
        assertThatCode(() -> consumer.recibir(normal)).doesNotThrowAnyException();
    }

    @Test
    void stockInvalidoLanzaEventoInvalido() {
        StockKafkaConsumer consumer = new StockKafkaConsumer();
        ConsumerRecord<String, StockEventoDto> malo = new ConsumerRecord<>("catalogo.stock", 0, 0L, "9",
                new StockEventoDto(9L, "Mouse", -1, LocalDateTime.now()));

        assertThatThrownBy(() -> consumer.recibir(malo)).isInstanceOf(EventoInvalidoException.class);
    }

    @Test
    void dltLeeLasCabecerasDeError() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("pedidos.eventos.DLT", 0, 0L, "1", "{}");
        record.headers().add(new RecordHeader("kafka_dlt-exception-message",
                "El evento no trae pedidoId".getBytes(StandardCharsets.UTF_8)));

        assertThat(DltKafkaConsumer.cabecera(record, "kafka_dlt-exception-message"))
                .isEqualTo("El evento no trae pedidoId");
        assertThat(DltKafkaConsumer.cabecera(record, "no-existe")).isEqualTo("desconocido");
        assertThatCode(() -> new DltKafkaConsumer().recibir(record)).doesNotThrowAnyException();
    }
}
