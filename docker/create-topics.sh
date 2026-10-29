#!/bin/bash
# =================================================================================================
# ARCHIVO: docker/create-topics.sh
# =================================================================================================
# DESCRIPCIÓN DE LO REALIZADO EN ESTE SCRIPT:
# 1. Creación automatizada de los 4 tópicos de negocio de Apache Kafka según la sección 05 del PDF:
#    - activation.requested
#    - billing.events
#    - provisioning.events
#    - activation.events
# 2. Creación de los tópicos para Colas de Mensajes Muertos (DLQ):
#    - activation.requested.dlq
#    - billing.events.dlq
#    - provisioning.events.dlq
#    - activation.events.dlq
# 3. Configuración estricta según el documento de arquitectura:
#    - 3 particiones por tópico (para permitir balanceo en grupos de consumidores y escalamiento).
#    - Factor de replicación 1 (cluster local de un broker KRaft).
#    - Retención de 7 días (retention.ms=604800000) coincidente con el TTL de MongoDB.
# 4. Operación idempotente usando el flag `--if-not-exists`.
# =================================================================================================

set -e

BOOTSTRAP_SERVER="kafka:9092"
RETENTION_MS=604800000 # 7 días en milisegundos

echo "Esperando que el broker Kafka esté disponible en ${BOOTSTRAP_SERVER}..."
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --list > /dev/null 2>&1; do
  echo "Kafka no responde aún, reintentando en 2 segundos..."
  sleep 2
done

echo "Broker Kafka disponible. Creando tópicos con 3 particiones y retención de 7 días..."

TOPICS=(
  "activation.requested"
  "activation.requested.dlq"
  "billing.events"
  "billing.events.dlq"
  "provisioning.events"
  "provisioning.events.dlq"
  "activation.events"
  "activation.events.dlq"
)

for topic in "${TOPICS[@]}"; do
  echo "Verificando/Creando tópico: ${topic}"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server ${BOOTSTRAP_SERVER} \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=${RETENTION_MS}
done

echo "================================================================="
echo "Lista final de tópicos en el cluster:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --list
echo "================================================================="
echo "Inicialización de tópicos completada exitosamente."
