package com.pedidos360.admin.kafka;

import com.pedidos360.admin.common.exception.ResourceNotFoundException;
import com.pedidos360.admin.kafka.dto.TopicRequestDto;
import com.pedidos360.admin.kafka.dto.TopicUpdateDto;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KafkaAdminServiceTest {

    private Admin admin;
    private KafkaAdminService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        service = new KafkaAdminService(admin, "pedidos.eventos", "catalogo.stock", "pedidos.eventos.DLT");
    }

    private TopicRequestDto request(String nombre, int particiones, int replicas) {
        return new TopicRequestDto(nombre, particiones, replicas, "delete", 86400000L, null);
    }

    private void topicosExistentes(Set<String> nombres) {
        ListTopicsResult resultado = mock(ListTopicsResult.class);
        when(resultado.names()).thenReturn(KafkaFuture.completedFuture(nombres));
        when(admin.listTopics()).thenReturn(resultado);
    }

    // ---------- Crear tópico ----------
    @Test
    void crearTopicoNuevoLoEnviaAKafkaConSuConfiguracion() {
        topicosExistentes(Set.of("pedidos.eventos"));
        CreateTopicsResult creacion = mock(CreateTopicsResult.class);
        when(creacion.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(creacion);

        String creado = service.crearTopico(request("ofertas.eventos", 3, 2));

        assertThat(creado).isEqualTo("ofertas.eventos");
        verify(admin).createTopics(argThat((Collection<NewTopic> topicos) -> {
            NewTopic topico = topicos.iterator().next();
            return topico.name().equals("ofertas.eventos")
                    && topico.numPartitions() == 3
                    && topico.replicationFactor() == 2
                    && "delete".equals(topico.configs().get("cleanup.policy"))
                    && "86400000".equals(topico.configs().get("retention.ms"));
        }));
    }

    @Test
    void crearTopicoQueYaExisteLanzaIllegalState() {
        topicosExistentes(Set.of("pedidos.eventos"));

        assertThatThrownBy(() -> service.crearTopico(request("pedidos.eventos", 3, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ya existe");
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void crearTopicoConNombreInternoLanzaIllegalArgument() {
        assertThatThrownBy(() -> service.crearTopico(request("__mi_topico", 1, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reservados");
        verifyNoInteractions(admin);
    }

    @Test
    void crearTopicoConMinInsyncMayorQueReplicasLanzaIllegalArgument() {
        topicosExistentes(Set.of());
        TopicRequestDto req = new TopicRequestDto("nuevo", 1, 1, null, null, 2);

        assertThatThrownBy(() -> service.crearTopico(req))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minInsyncReplicas");
    }

    // ---------- Modificar tópico ----------
    @Test
    void modificarSinCambiosLanzaIllegalArgument() {
        assertThatThrownBy(() -> service.actualizarTopico("pedidos.eventos", new TopicUpdateDto(null, null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("al menos un cambio");
        verifyNoInteractions(admin);
    }

    @Test
    void reducirParticionesLanzaIllegalArgument() {
        TopicDescription descripcion = mock(TopicDescription.class);
        when(descripcion.partitions()).thenReturn(List.of(
                new org.apache.kafka.common.TopicPartitionInfo(0, null, List.of(), List.of()),
                new org.apache.kafka.common.TopicPartitionInfo(1, null, List.of(), List.of()),
                new org.apache.kafka.common.TopicPartitionInfo(2, null, List.of(), List.of())));
        org.apache.kafka.clients.admin.DescribeTopicsResult resultado =
                mock(org.apache.kafka.clients.admin.DescribeTopicsResult.class);
        when(resultado.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("ofertas", descripcion)));
        when(admin.describeTopics(anyCollection())).thenReturn(resultado);

        assertThatThrownBy(() -> service.actualizarTopico("ofertas", new TopicUpdateDto(2, null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No se pueden reducir");
    }

    // ---------- Eliminar tópico ----------
    @Test
    void eliminarTopicoDelSistemaLanzaIllegalState() {
        assertThatThrownBy(() -> service.eliminarTopico("pedidos.eventos"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Pedidos360");
        assertThatThrownBy(() -> service.eliminarTopico("catalogo.stock"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> service.eliminarTopico("pedidos.eventos.DLT"))
                .isInstanceOf(IllegalStateException.class);
        verifyNoInteractions(admin);
    }

    @Test
    void eliminarTopicoInexistenteLanzaNotFound() {
        org.apache.kafka.clients.admin.DescribeTopicsResult resultado =
                mock(org.apache.kafka.clients.admin.DescribeTopicsResult.class);
        KafkaFutureImpl<Map<String, TopicDescription>> fallido = new KafkaFutureImpl<>();
        fallido.completeExceptionally(new UnknownTopicOrPartitionException("no existe"));
        when(resultado.allTopicNames()).thenReturn(fallido);
        when(admin.describeTopics(anyCollection())).thenReturn(resultado);

        assertThatThrownBy(() -> service.eliminarTopico("fantasma"))
                .isInstanceOf(ResourceNotFoundException.class);
        verify(admin, never()).deleteTopics(anyCollection());
    }

    // ---------- Consumer groups ----------
    @Test
    void eliminarGrupoConMiembrosConectadosLanzaIllegalState() {
        DeleteConsumerGroupsResult resultado = mock(DeleteConsumerGroupsResult.class);
        KafkaFutureImpl<Void> fallido = new KafkaFutureImpl<>();
        fallido.completeExceptionally(new GroupNotEmptyException("hay miembros"));
        when(resultado.all()).thenReturn(fallido);
        when(admin.deleteConsumerGroups(anyCollection())).thenReturn(resultado);

        assertThatThrownBy(() -> service.eliminarGrupo("notificaciones-group"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("miembros conectados");
    }

    @Test
    void eliminarGrupoVacioFunciona() {
        DeleteConsumerGroupsResult resultado = mock(DeleteConsumerGroupsResult.class);
        when(resultado.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteConsumerGroups(anyCollection())).thenReturn(resultado);

        service.eliminarGrupo("grupo-viejo");

        verify(admin).deleteConsumerGroups(anyCollection());
    }
}
