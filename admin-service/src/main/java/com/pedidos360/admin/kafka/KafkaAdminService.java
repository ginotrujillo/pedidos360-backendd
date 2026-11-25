package com.pedidos360.admin.kafka;

import com.pedidos360.admin.common.exception.KafkaAdminException;
import com.pedidos360.admin.common.exception.ResourceNotFoundException;
import com.pedidos360.admin.kafka.dto.ConsumerGroupDetail;
import com.pedidos360.admin.kafka.dto.ConsumerGroupSummary;
import com.pedidos360.admin.kafka.dto.TopicDetailResponse;
import com.pedidos360.admin.kafka.dto.TopicInfoResponse;
import com.pedidos360.admin.kafka.dto.TopicRequestDto;
import com.pedidos360.admin.kafka.dto.TopicUpdateDto;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Lógica del administrador de Kafka. Toda la interacción con el clúster pasa por el cliente Admin.
 *
 * Reglas de validación de negocio (además de Bean Validation en los DTO):
 *  - No se permiten nombres de tópico que empiecen con "__" (internos de Kafka) ni "." / "..".
 *  - No se puede crear un tópico que ya existe (409) ni modificar/eliminar uno que no existe (404).
 *  - No se pueden eliminar los tópicos que usa Pedidos360 (409).
 *  - Las particiones solo se pueden aumentar, nunca reducir (400).
 *  - Un consumer group solo se puede eliminar si no tiene miembros conectados (409).
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private static final long TIMEOUT_SEGUNDOS = 15;

    /** Configuraciones que siempre se muestran en el detalle de un tópico (además de las modificadas). */
    private static final Set<String> CONFIGS_RELEVANTES = Set.of(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.RETENTION_MS_CONFIG,
            TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
            TopicConfig.SEGMENT_MS_CONFIG,
            TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG);

    private final Admin admin;
    private final Set<String> topicosDelSistema;

    public KafkaAdminService(Admin admin,
                             @Value("${app.kafka.topics.pedidos-eventos}") String topicPedidos,
                             @Value("${app.kafka.topics.catalogo-stock}") String topicStock,
                             @Value("${app.kafka.topics.pedidos-dlt}") String topicDlt) {
        this.admin = admin;
        this.topicosDelSistema = Set.of(topicPedidos, topicStock, topicDlt);
    }

    // ===================== TÓPICOS =====================

    public List<TopicInfoResponse> listarTopicos() {
        Set<String> nombres = esperar(admin.listTopics(new ListTopicsOptions().listInternal(false)).names(), "los tópicos");
        if (nombres.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> descripciones = esperar(admin.describeTopics(nombres).allTopicNames(), "los tópicos");
        return descripciones.values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(d -> new TopicInfoResponse(d.name(), d.partitions().size(), factorReplicacion(d)))
                .toList();
    }

    public TopicDetailResponse obtenerTopico(String nombre) {
        TopicDescription descripcion = describirTopico(nombre);

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Map<ConfigResource, Config> configs = esperar(
                admin.describeConfigs(List.of(recurso)).all(), "el tópico '" + nombre + "'");
        Map<String, String> relevantes = new TreeMap<>();
        Config config = configs.get(recurso);
        if (config != null) {
            for (ConfigEntry entrada : config.entries()) {
                boolean modificada = entrada.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
                if (modificada || CONFIGS_RELEVANTES.contains(entrada.name())) {
                    relevantes.put(entrada.name(), entrada.value());
                }
            }
        }

        List<TopicDetailResponse.PartitionInfo> particiones = descripcion.partitions().stream()
                .map(p -> new TopicDetailResponse.PartitionInfo(
                        p.partition(),
                        p.leader() == null ? -1 : p.leader().id(),
                        p.replicas().stream().map(Node::id).toList(),
                        p.isr().stream().map(Node::id).toList()))
                .toList();

        return new TopicDetailResponse(nombre, descripcion.partitions().size(),
                factorReplicacion(descripcion), particiones, relevantes);
    }

    public String crearTopico(TopicRequestDto req) {
        String nombre = req.name().trim();
        validarNombre(nombre);
        if (existeTopico(nombre)) {
            throw new IllegalStateException("El tópico '" + nombre + "' ya existe");
        }
        if (req.minInsyncReplicas() != null && req.minInsyncReplicas() > req.replicationFactor()) {
            throw new IllegalArgumentException(
                    "minInsyncReplicas no puede ser mayor que el factor de replicación");
        }

        Map<String, String> configs = configuraciones(req.cleanupPolicy(), req.retentionMs());
        if (req.minInsyncReplicas() != null) {
            configs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(req.minInsyncReplicas()));
        }

        NewTopic nuevo = new NewTopic(nombre, req.partitions(), req.replicationFactor().shortValue());
        nuevo.configs(configs);
        esperar(admin.createTopics(List.of(nuevo)).all(), "el tópico '" + nombre + "'");
        log.info("[ADMIN-KAFKA] Tópico creado: {} (particiones={}, réplicas={}, configs={})",
                nombre, req.partitions(), req.replicationFactor(), configs);
        return nombre;
    }

    public String actualizarTopico(String nombre, TopicUpdateDto req) {
        validarNombre(nombre);
        if (req.partitions() == null && req.cleanupPolicy() == null && req.retentionMs() == null) {
            throw new IllegalArgumentException(
                    "Debe indicar al menos un cambio: partitions, cleanupPolicy o retentionMs");
        }
        TopicDescription actual = describirTopico(nombre);
        String recurso = "el tópico '" + nombre + "'";

        if (req.partitions() != null) {
            int particionesActuales = actual.partitions().size();
            if (req.partitions() < particionesActuales) {
                throw new IllegalArgumentException("No se pueden reducir las particiones (actuales: "
                        + particionesActuales + ", pedidas: " + req.partitions() + ")");
            }
            if (req.partitions() > particionesActuales) {
                Map<String, NewPartitions> aumento = Map.of(nombre, NewPartitions.increaseTo(req.partitions()));
                esperar(admin.createPartitions(aumento).all(), recurso);
                log.info("[ADMIN-KAFKA] Particiones de {} aumentadas: {} -> {}",
                        nombre, particionesActuales, req.partitions());
            }
        }

        Map<String, String> configs = configuraciones(req.cleanupPolicy(), req.retentionMs());
        if (!configs.isEmpty()) {
            Collection<AlterConfigOp> operaciones = configs.entrySet().stream()
                    .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                    .toList();
            Map<ConfigResource, Collection<AlterConfigOp>> cambios =
                    Map.of(new ConfigResource(ConfigResource.Type.TOPIC, nombre), operaciones);
            esperar(admin.incrementalAlterConfigs(cambios).all(), recurso);
            log.info("[ADMIN-KAFKA] Configuración de {} modificada: {}", nombre, configs);
        }
        return nombre;
    }

    public void eliminarTopico(String nombre) {
        if (topicosDelSistema.contains(nombre)) {
            throw new IllegalStateException("No se puede eliminar el tópico '" + nombre
                    + "' porque lo usa el sistema Pedidos360");
        }
        validarNombre(nombre);
        describirTopico(nombre);   // 404 si no existe
        esperar(admin.deleteTopics(List.of(nombre)).all(), "el tópico '" + nombre + "'");
        log.info("[ADMIN-KAFKA] Tópico eliminado: {}", nombre);
    }

    // ===================== CONSUMER GROUPS =====================

    public List<ConsumerGroupSummary> listarGrupos() {
        Collection<ConsumerGroupListing> grupos = esperar(admin.listConsumerGroups().all(), "los consumer groups");
        return grupos.stream()
                .map(g -> new ConsumerGroupSummary(g.groupId(), g.state().map(Object::toString).orElse("DESCONOCIDO")))
                .sorted(Comparator.comparing(ConsumerGroupSummary::groupId))
                .toList();
    }

    public ConsumerGroupDetail obtenerGrupo(String groupId) {
        String recurso = "el consumer group '" + groupId + "'";
        Map<String, ConsumerGroupDescription> descripciones =
                esperar(admin.describeConsumerGroups(List.of(groupId)).all(), recurso);
        ConsumerGroupDescription descripcion = descripciones.get(groupId);
        if (descripcion == null || "DEAD".equalsIgnoreCase(String.valueOf(descripcion.state()))) {
            throw new ResourceNotFoundException("El consumer group '" + groupId + "' no existe");
        }

        List<ConsumerGroupDetail.MemberInfo> miembros = descripcion.members().stream()
                .map(m -> new ConsumerGroupDetail.MemberInfo(
                        m.clientId(),
                        m.host(),
                        m.assignment().topicPartitions().stream()
                                .map(tp -> tp.topic() + "-" + tp.partition())
                                .sorted()
                                .toList()))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> confirmados =
                esperar(admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(), recurso);

        List<ConsumerGroupDetail.OffsetInfo> offsets = new ArrayList<>();
        long lagTotal = 0;
        if (!confirmados.isEmpty()) {
            Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
            for (TopicPartition tp : confirmados.keySet()) {
                consulta.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> finales =
                    esperar(admin.listOffsets(consulta).all(), recurso);

            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : confirmados.entrySet()) {
                if (e.getValue() == null || finales.get(e.getKey()) == null) {
                    continue;
                }
                long actual = e.getValue().offset();
                long fin = finales.get(e.getKey()).offset();
                long lag = Math.max(0, fin - actual);
                lagTotal += lag;
                offsets.add(new ConsumerGroupDetail.OffsetInfo(
                        e.getKey().topic(), e.getKey().partition(), actual, fin, lag));
            }
            offsets.sort(Comparator.comparing(ConsumerGroupDetail.OffsetInfo::topic)
                    .thenComparingInt(ConsumerGroupDetail.OffsetInfo::partition));
        }
        return new ConsumerGroupDetail(groupId, String.valueOf(descripcion.state()), miembros, offsets, lagTotal);
    }

    public void eliminarGrupo(String groupId) {
        esperar(admin.deleteConsumerGroups(List.of(groupId)).all(), "el consumer group '" + groupId + "'");
        log.info("[ADMIN-KAFKA] Consumer group eliminado: {}", groupId);
    }

    // ===================== AUXILIARES =====================

    private TopicDescription describirTopico(String nombre) {
        Map<String, TopicDescription> descripciones =
                esperar(admin.describeTopics(List.of(nombre)).allTopicNames(), "el tópico '" + nombre + "'");
        TopicDescription descripcion = descripciones.get(nombre);
        if (descripcion == null) {
            throw new ResourceNotFoundException("El tópico '" + nombre + "' no existe");
        }
        return descripcion;
    }

    private boolean existeTopico(String nombre) {
        return esperar(admin.listTopics().names(), "los tópicos").contains(nombre);
    }

    private void validarNombre(String nombre) {
        if (nombre.startsWith("__")) {
            throw new IllegalArgumentException("Los nombres que empiezan con '__' están reservados por Kafka");
        }
        if (nombre.equals(".") || nombre.equals("..")) {
            throw new IllegalArgumentException("Nombre de tópico inválido: '" + nombre + "'");
        }
    }

    private int factorReplicacion(TopicDescription descripcion) {
        return descripcion.partitions().isEmpty() ? 0 : descripcion.partitions().get(0).replicas().size();
    }

    private Map<String, String> configuraciones(String cleanupPolicy, Long retentionMs) {
        Map<String, String> configs = new HashMap<>();
        if (cleanupPolicy != null) {
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, cleanupPolicy);
        }
        if (retentionMs != null) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(retentionMs));
        }
        return configs;
    }

    /** Espera el resultado de Kafka y traduce sus errores a las excepciones del dominio (400/404/409/503). */
    private <T> T esperar(KafkaFuture<T> futuro, String recurso) {
        try {
            return futuro.get(TIMEOUT_SEGUNDOS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaAdminException("La operación sobre " + recurso + " fue interrumpida", e);
        } catch (TimeoutException e) {
            throw new KafkaAdminException("Kafka no respondió a tiempo al consultar " + recurso, e);
        } catch (ExecutionException e) {
            throw traducir(e.getCause() != null ? e.getCause() : e, recurso);
        }
    }

    private RuntimeException traducir(Throwable causa, String recurso) {
        if (causa instanceof TopicExistsException) {
            return new IllegalStateException("Ya existe " + recurso);
        }
        if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
            return new ResourceNotFoundException("No existe " + recurso);
        }
        if (causa instanceof GroupNotEmptyException) {
            return new IllegalStateException(
                    "No se puede eliminar " + recurso + " porque tiene miembros conectados");
        }
        if (causa instanceof InvalidTopicException
                || causa instanceof InvalidPartitionsException
                || causa instanceof InvalidReplicationFactorException
                || causa instanceof InvalidConfigurationException
                || causa instanceof InvalidRequestException) {
            return new IllegalArgumentException(causa.getMessage());
        }
        return new KafkaAdminException("Error de Kafka sobre " + recurso + ": " + causa.getMessage(), causa);
    }
}
