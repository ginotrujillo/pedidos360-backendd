package com.pedidos360.admin.kafka;

import com.pedidos360.admin.dto.AdminResponse;
import com.pedidos360.admin.kafka.dto.ConsumerGroupDetail;
import com.pedidos360.admin.kafka.dto.ConsumerGroupSummary;
import com.pedidos360.admin.kafka.dto.TopicDetailResponse;
import com.pedidos360.admin.kafka.dto.TopicInfoResponse;
import com.pedidos360.admin.kafka.dto.TopicRequestDto;
import com.pedidos360.admin.kafka.dto.TopicUpdateDto;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Microservicio administrador de Kafka (solo rol Admin).
 *
 *  GET    /api/admin/kafka/topics                    listar tópicos
 *  GET    /api/admin/kafka/topics/{name}             detalle: particiones, líder, ISR y configuración
 *  POST   /api/admin/kafka/topics                    crear tópico (particiones, réplicas, cleanup.policy, retención)
 *  PUT    /api/admin/kafka/topics/{name}             modificar (aumentar particiones, retención, cleanup.policy)
 *  DELETE /api/admin/kafka/topics/{name}             eliminar tópico
 *  GET    /api/admin/kafka/consumer-groups           listar consumer groups
 *  GET    /api/admin/kafka/consumer-groups/{id}      detalle: miembros, particiones asignadas y lag
 *  DELETE /api/admin/kafka/consumer-groups/{id}      eliminar grupo (solo si no tiene miembros conectados)
 */
@RestController
@RequestMapping("/api/admin/kafka")
@PreAuthorize("hasRole('Admin')")
public class KafkaAdminController {

    private static final String NOMBRE_VALIDO = "^[a-zA-Z0-9._-]+$";
    private static final String MSG_NOMBRE = "Nombre inválido: solo letras, números, '.', '-' y '_'";

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    // ---------- Tópicos ----------
    @GetMapping("/topics")
    public List<TopicInfoResponse> listarTopicos() {
        return kafkaAdminService.listarTopicos();
    }

    @GetMapping("/topics/{name}")
    public TopicDetailResponse verTopico(
            @PathVariable("name") @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MSG_NOMBRE) String name) {
        return kafkaAdminService.obtenerTopico(name);
    }

    @PostMapping("/topics")
    public ResponseEntity<AdminResponse> crearTopico(@Valid @RequestBody TopicRequestDto request) {
        String nombre = kafkaAdminService.crearTopico(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new AdminResponse("CREAR_TOPICO", nombre, "Tópico creado correctamente"));
    }

    @PutMapping("/topics/{name}")
    public AdminResponse modificarTopico(
            @PathVariable("name") @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MSG_NOMBRE) String name,
            @Valid @RequestBody TopicUpdateDto request) {
        kafkaAdminService.actualizarTopico(name, request);
        return new AdminResponse("MODIFICAR_TOPICO", name, "Tópico modificado correctamente");
    }

    @DeleteMapping("/topics/{name}")
    public AdminResponse eliminarTopico(
            @PathVariable("name") @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MSG_NOMBRE) String name) {
        kafkaAdminService.eliminarTopico(name);
        return new AdminResponse("ELIMINAR_TOPICO", name, "Tópico eliminado correctamente");
    }

    // ---------- Consumer groups ----------
    @GetMapping("/consumer-groups")
    public List<ConsumerGroupSummary> listarGrupos() {
        return kafkaAdminService.listarGrupos();
    }

    @GetMapping("/consumer-groups/{id}")
    public ConsumerGroupDetail verGrupo(
            @PathVariable("id") @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MSG_NOMBRE) String id) {
        return kafkaAdminService.obtenerGrupo(id);
    }

    @DeleteMapping("/consumer-groups/{id}")
    public AdminResponse eliminarGrupo(
            @PathVariable("id") @NotBlank @Pattern(regexp = NOMBRE_VALIDO, message = MSG_NOMBRE) String id) {
        kafkaAdminService.eliminarGrupo(id);
        return new AdminResponse("ELIMINAR_CONSUMER_GROUP", id, "Consumer group eliminado correctamente");
    }
}
