# Pedidos360 — Backend

Proyecto académico — **DSY1107 Desarrollo Cloud Native I**

**Integrante:** Gino Alfonso Trujillo Mariangeli (trabajo individual)

Este repositorio contiene el **backend** de Pedidos360. El frontend (Angular) vive en su propio repositorio: `pedidos360-frontend`.

---

## Descripción

Pedidos360 es una aplicación de gestión de pedidos y catálogo de productos, con autenticación y autorización por roles mediante Microsoft Entra ID. El backend está dividido en **tres microservicios** que se comunican de forma asíncrona con **RabbitMQ** y **Kafka**.

Reglas de negocio:
- Estados del pedido: `CREADO → ACEPTADO → EN_PREPARACION → DESPACHADO → ENTREGADO` (con posibilidad de `CANCELADO`).
- Un pedido no puede pasar a `DESPACHADO` si no fue previamente `ACEPTADO`.
- Al aceptar un pedido, el stock de los productos involucrados disminuye automáticamente.
- Si un pedido aceptado se cancela, el stock se restaura.

## Arquitectura

```
Angular + MSAL ──► Microsoft Entra ID (JWT) ──► AWS API Gateway (JWT Authorizer)
                                                        │
                       ┌────────────────────────────────┴───────────────┐
                       ▼                                                ▼
              pedidos-service :8080                            admin-service :8082
              REST + seguridad JWT                             administrador RabbitMQ
              base de datos (H2 / RDS)                         administrador Kafka
                 │          │
        productor│          │productor
                 ▼          ▼
            RabbitMQ      Kafka
         (clúster 2 nodos) (2 brokers + Zookeeper)
                 │          │
        consumer │          │ consumer
                 ▼          ▼
              eventos-service :8081
              consumers RabbitMQ (ACK manual + DLQ)
              consumers Kafka (consumer groups + reintentos + DLT)
```

| Microservicio | Puerto | Responsabilidad |
|---|---|---|
| `pedidos-service` | 8080 | API REST de catálogo y pedidos, validación JWT, base de datos, **productores** hacia RabbitMQ y Kafka |
| `eventos-service` | 8081 | **Consumidores** de RabbitMQ (los de siempre, con ACK manual y DLQ) y de Kafka (con `@KafkaListener`, consumer groups, reintentos y DLT) |
| `admin-service` | 8082 | **Administrador** de RabbitMQ (colas, exchanges, bindings) y de Kafka (tópicos y consumer groups). Solo rol `Admin` |

Cada microservicio es un módulo Maven independiente que se compila y se despliega por separado. No comparten código: los eventos tienen un DTO propio en cada servicio, y Kafka traduce entre ellos con un nombre lógico de tipo (`pedidoEvento`, `stockEvento`).

## Kafka

### Tópicos

| Tópico | Particiones | Réplicas | `cleanup.policy` | Retención | Clave | Uso |
|---|:---:|:---:|---|---|---|---|
| `pedidos.eventos` | 3 | 2 | `delete` | 7 días | id del pedido | Un evento por cada cambio de estado de un pedido |
| `catalogo.stock` | 2 | 2 | `compact` | — (queda el último valor por clave) | id del producto | Stock vigente de cada producto |
| `pedidos.eventos.DLT` | 3 | 2 | `delete` | 30 días | la del mensaje original | Mensajes que no se pudieron procesar |

La clave es el id del pedido: así todos los eventos de un mismo pedido caen en la misma partición y se consumen en orden.

Los tópicos se crean de forma replicable de dos maneras equivalentes: el contenedor `kafka-init` de `docker/kafka/docker-compose.kafka.yml` y la clase `KafkaTopicConfig` de `pedidos-service` (solo crea los que faltan).

### Productores (`pedidos-service`)

`KafkaEventProducer` publica con `KafkaTemplate`, serializando el DTO a JSON (`JacksonJsonSerializer`), `acks=all` e idempotencia activada. El envío es asíncrono y nunca rompe la operación de negocio: si Kafka no responde, el error queda en el log y el pedido se guarda igual.

### Consumidores y consumer groups (`eventos-service`)

| Consumer | Tópico | Consumer group |
|---|---|---|
| `NotificacionKafkaConsumer` | `pedidos.eventos` | `notificaciones-group` |
| `AuditoriaKafkaConsumer` | `pedidos.eventos` | `auditoria-group` |
| `StockKafkaConsumer` | `catalogo.stock` | `stock-group` |
| `DltKafkaConsumer` | `pedidos.eventos.DLT` | `dlt-monitor-group` |

Dos grupos distintos sobre el mismo tópico reciben **cada uno su propia copia** de todos los eventos. Dentro de un mismo grupo, las particiones se reparten entre los consumers: si se levantan dos instancias de `eventos-service`, las 3 particiones de `pedidos.eventos` se dividen entre ellas.

### Manejo de errores

```
mensaje ──► deserializar ──► validar contenido ──► procesar
               │                   │                  │
        JSON roto            EventoInvalido      error transitorio
               │                   │                  │
               │                   │          3 reintentos (1 s entre cada uno)
               ▼                   ▼                  ▼
          ──────────── pedidos.eventos.DLT (+ log del motivo) ────────────
```

- **JSON roto:** `ErrorHandlingDeserializer` evita que el consumer se caiga; el mensaje va directo al DLT.
- **Contenido inválido** (sin `pedidoId`, sin estado, sin email, stock negativo): `EventoInvalidoException`, sin reintentos, directo al DLT.
- **Error transitorio** (por ejemplo servicio de correo caído): 3 reintentos con 1 segundo de espera; si siguen fallando, va al DLT.
- Cada reintento y cada envío al DLT quedan en el log, y `DltKafkaConsumer` registra el motivo de cada mensaje muerto.

Para ver los reintentos y el DLT en una demostración, arrancar `eventos-service` con `--app.kafka.demo.fallar-notificaciones=true`.

## RabbitMQ

Exchanges `pedidos.direct` y `pedidos.topic`, tres colas (`stock`, `notificaciones`, `auditoria`), cada una con su DLQ. Los consumers usan **ACK manual**: `basicAck` si todo sale bien, `basicNack` sin reencolar si falla (el mensaje va a la DLQ).

`pedidos-service` solo declara los exchanges y publica. Las colas, DLQ y bindings los declara `eventos-service`, por eso **debe arrancar al menos una vez antes** que `pedidos-service`: si no, los mensajes publicados sin cola enlazada se pierden.

El clúster de 2 nodos para la nube está en `deploy/cloud/`.

## Administradores (`admin-service`, solo rol `Admin`)

**RabbitMQ** — `/api/admin/rabbitmq`
```
POST   /queues              crear cola (opcional: DLQ, TTL, max-length)
GET    /queues/{name}       mensajes en espera y consumidores
DELETE /queues/{name}       eliminar cola
POST   /exchanges           crear exchange (direct, topic, fanout)
DELETE /exchanges/{name}    eliminar exchange
POST   /bindings            crear binding
DELETE /bindings            eliminar binding
```

**Kafka** — `/api/admin/kafka`
```
GET    /topics                    listar tópicos
GET    /topics/{name}             detalle: particiones, líder, ISR y configuración
POST   /topics                    crear (particiones, réplicas, cleanupPolicy, retentionMs, minInsyncReplicas)
PUT    /topics/{name}             modificar (aumentar particiones, retentionMs, cleanupPolicy)
DELETE /topics/{name}             eliminar
GET    /consumer-groups           listar consumer groups
GET    /consumer-groups/{id}      miembros, particiones asignadas y lag por partición
DELETE /consumer-groups/{id}      eliminar (solo si no tiene miembros conectados)
```

Ejemplo para crear un tópico:
```json
POST /api/admin/kafka/topics
{ "name": "ofertas.eventos", "partitions": 3, "replicationFactor": 2,
  "cleanupPolicy": "delete", "retentionMs": 86400000 }
```

Reglas de validación: nombres solo con letras, números, `.`, `-` y `_`; no se aceptan nombres que empiecen con `__`; no se puede crear lo que ya existe (409) ni tocar lo que no existe (404); las particiones solo pueden aumentar; los tópicos y colas que usa Pedidos360 no se pueden eliminar (409).

## Roles y permisos

| Acción | Admin | Operador | Cliente |
|---|:---:|:---:|:---:|
| Ver catálogo | ✅ | ✅ | ✅ |
| Crear/editar producto | ✅ | ❌ | ❌ |
| Crear pedido | ✅ | ✅ | ✅ |
| Ver pedidos propios | ✅ | ✅ | ✅ |
| Ver todos los pedidos | ✅ | ✅ | ❌ |
| Cambiar estado de un pedido | ✅ | ✅ | ❌ |
| Administrar RabbitMQ y Kafka | ✅ | ❌ | ❌ |

La autorización se aplica con `@PreAuthorize` a partir del claim `roles` del token de Entra ID. `pedidos-service` y `admin-service` validan el JWT (issuer y audience).

## Estructura del repositorio

```
pedidos360-backend/
├── pom.xml                      # POM padre (agrupa los 3 módulos)
├── pedidos-service/             # API REST + productores RabbitMQ y Kafka
│   └── src/main/java/com/pedidos360/pedidos/{catalog,orders,config,common,messaging}
├── eventos-service/             # Consumers RabbitMQ y Kafka
│   └── src/main/java/com/pedidos360/eventos/{rabbit,kafka}
├── admin-service/               # Administradores RabbitMQ y Kafka
│   └── src/main/java/com/pedidos360/admin/{rabbit,kafka,config,common,dto}
├── docker/
│   ├── kafka/                   # Zookeeper + 2 brokers + kafka-init, y Kafka-ui
│   └── rabbitmq/                # RabbitMQ local de un nodo
└── deploy/cloud/                # Clúster RabbitMQ de 2 nodos y script de arranque en EC2
```

## Cómo ejecutar en local

Requisitos: Java 21, Docker. Maven va incluido como `mvnw`.

**1. Infraestructura**
```bash
docker compose -f docker/rabbitmq/docker-compose.local.yml up -d
docker compose -f docker/kafka/docker-compose.kafka.yml up -d
docker compose -f docker/kafka/docker-compose.kafka-ui.yml up -d      # opcional
```
- RabbitMQ Management: http://localhost:15672 (admin / admin123, solo local)
- Kafka-ui: http://localhost:8085

**2. Compilar y probar** (desde la raíz del repositorio)
```bash
./mvnw clean package
```

**3. Arrancar los microservicios** (cada uno en su terminal, en este orden)
```bash
./mvnw -pl eventos-service spring-boot:run
./mvnw -pl pedidos-service spring-boot:run
./mvnw -pl admin-service   spring-boot:run
```

Por defecto corren con el perfil `dev` (H2 en memoria, RabbitMQ y Kafka en localhost). Con un solo broker Kafka, definir `KAFKA_REPLICAS=1` antes de arrancar `pedidos-service`.

Para producción con PostgreSQL (RDS): `--spring.profiles.active=prod` con `DB_HOST`, `DB_USER`, `DB_PASSWORD`, `RABBITMQ_ADDRESSES`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD` y `KAFKA_BOOTSTRAP_SERVERS` como variables de entorno. En la EC2, `deploy/cloud/arrancar-backend.sh` arranca los tres servicios con el perfil `cloud`.

## Pruebas

```bash
./mvnw test
```

Pruebas unitarias con JUnit 5 y Mockito, que **no necesitan RabbitMQ, Kafka ni base de datos**:

| Módulo | Qué se prueba |
|---|---|
| `pedidos-service` | Reglas de negocio de pedidos (transiciones de estado, descuento y restauración de stock, creación), publicador RabbitMQ y productor Kafka (claves, tópicos, tolerancia a fallos) |
| `eventos-service` | Validación de eventos Kafka, los cuatro consumers de Kafka, los tres consumers de RabbitMQ (`basicAck` / `basicNack` sin reencolar) |
| `admin-service` | Administrador de Kafka (crear, modificar, eliminar tópicos y grupos, errores de Kafka traducidos a 404/409/400), administrador de RabbitMQ, validación de los DTO |

## Frontend

Repositorio aparte: `pedidos360-frontend` (Angular 22, `@azure/msal-angular`). La URL de la API se configura en `src/app/auth-config.ts`.

## Tecnologías

| Capa | Tecnología |
|---|---|
| Autenticación | Microsoft Entra ID, OAuth 2.0 / OpenID Connect (Authorization Code + PKCE) |
| Puerta de entrada | AWS API Gateway (HTTP API), JWT Authorizer, CORS |
| Backend | Java 21, Spring Boot 4.1.1, Spring Security (Resource Server, JWT), Maven multi-módulo |
| Mensajería | RabbitMQ (clúster de 2 nodos), Apache Kafka (2 brokers + Zookeeper), Spring AMQP, Spring for Apache Kafka |
| Persistencia | Spring Data JPA / Hibernate, H2 (dev/cloud), PostgreSQL en AWS RDS (prod) |
| Infraestructura | Docker Compose, AWS EC2 (Amazon Linux 2023), AWS RDS, AWS API Gateway |
