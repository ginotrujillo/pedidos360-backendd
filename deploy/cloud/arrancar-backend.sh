#!/bin/bash
# Arranca los 3 microservicios en la EC2 con el perfil "cloud":
#   pedidos-service (8080) · eventos-service (8081) · admin-service (8082)
# Conectados a los 2 nodos del clúster RabbitMQ y a los 2 brokers Kafka.
# Lee usuario y clave de RabbitMQ desde el mismo .env del clúster (no se escriben en el repo).
#
# Antes: compilar con  ./mvnw clean package   desde la raíz del repositorio.
# Uso:   ./arrancar-backend.sh
set -e
cd "$(dirname "$0")"
set -a; source .env; set +a

export RABBITMQ_ADDRESSES="${RABBITMQ_ADDRESSES:-localhost:5672,localhost:5673}"
export RABBITMQ_USER="$RABBITMQ_USER"
export RABBITMQ_PASSWORD="$RABBITMQ_PASS"
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092,localhost:9093}"

jar_de() {
  ls ../../"$1"/target/"$1"-*.jar | grep -v original | head -1
}

# Evita instancias duplicadas peleando por los puertos
pkill -f "eventos-service" || true
pkill -f "pedidos-service" || true
pkill -f "admin-service" || true
sleep 2

# eventos-service primero: declara las colas y bindings de RabbitMQ que luego usa pedidos-service
nohup java -jar "$(jar_de eventos-service)" --spring.profiles.active=cloud "$@" > ~/eventos.log 2>&1 &
sleep 8
nohup java -jar "$(jar_de pedidos-service)" --spring.profiles.active=cloud "$@" > ~/pedidos.log 2>&1 &
nohup java -jar "$(jar_de admin-service)" --spring.profiles.active=cloud "$@" > ~/admin.log 2>&1 &

echo "Microservicios iniciados. Ver logs con:"
echo "  tail -f ~/pedidos.log    (API REST + productores)"
echo "  tail -f ~/eventos.log    (consumers RabbitMQ y Kafka)"
echo "  tail -f ~/admin.log      (administradores)"
