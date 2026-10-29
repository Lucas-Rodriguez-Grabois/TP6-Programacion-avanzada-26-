/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/index.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Punto de entrada principal para la librería `@poc/kafka-toolkit`.
 * 2. Re-exporta el módulo NestJS principal, los esquemas de bases de datos MongoDB y los servicios
 *    de infraestructura compartida:
 *    - `KafkaToolkitModule`
 *    - `IdempotencyService` y `ProcessedEvent`
 *    - `DlqRetryService`
 *    - `OutboxService` y `OutboxRecord`
 *    - `KafkaProducerService`
 * =================================================================================================
 */

export * from './kafka-toolkit.module';
export * from './idempotency/idempotency.service';
export * from './idempotency/processed-event.schema';
export * from './dlq-retry/dlq-retry.service';
export * from './outbox/outbox.service';
export * from './outbox/outbox.schema';
export * from './kafka-producer/kafka-producer.service';
