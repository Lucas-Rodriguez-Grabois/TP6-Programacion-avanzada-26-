/**
 * =================================================================================================
 * ARCHIVO: libs/contracts/src/topics.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición centralizada de las constantes de tópicos de Apache Kafka según la sección 05 del PDF.
 *    - TOPIC_ACTIVATION_REQUESTED = 'activation.requested'
 *    - TOPIC_BILLING_EVENTS       = 'billing.events'
 *    - TOPIC_PROVISIONING_EVENTS  = 'provisioning.events'
 *    - TOPIC_ACTIVATION_EVENTS    = 'activation.events'
 * 2. Definición de nombres para colas de mensajes muertos (Dead Letter Queues - DLQ) con sufijo `.dlq`.
 * 3. Identificadores de Consumer Groups para cada microservicio:
 *    - activation-api, billing-svc, provisioning-svc, notification-svc, crm-analytics, loyalty-group.
 * 4. Helper `getDlqTopicName` para derivar dinámicamente el nombre del tópico DLQ a partir de cualquier tópico base.
 * =================================================================================================
 */

export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export const DLQ_TOPICS = {
  ACTIVATION_REQUESTED_DLQ: 'activation.requested.dlq',
  BILLING_EVENTS_DLQ: 'billing.events.dlq',
  PROVISIONING_EVENTS_DLQ: 'provisioning.events.dlq',
  ACTIVATION_EVENTS_DLQ: 'activation.events.dlq',
} as const;

export const CONSUMER_GROUPS = {
  ACTIVATION_API: 'activation-api',
  BILLING_SERVICE: 'billing-svc',
  PROVISIONING_SERVICE: 'provisioning-svc',
  NOTIFICATION_SERVICE: 'notification-svc',
  CRM_ANALYTICS: 'crm-analytics',
  LOYALTY_SERVICE: 'loyalty-group',
} as const;

export function getDlqTopicName(topic: string): string {
  return `${topic}.dlq`;
}
