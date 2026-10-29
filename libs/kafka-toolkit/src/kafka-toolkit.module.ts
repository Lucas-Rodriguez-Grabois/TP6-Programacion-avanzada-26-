/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/kafka-toolkit.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del módulo NestJS `KafkaToolkitModule` para reutilización transversal en todos los servicios.
 * 2. Registra los esquemas Mongoose de `ProcessedEvent` (idempotencia) y `OutboxRecord` (outbox transaccional).
 * 3. Provee y exporta los 4 pilares de resiliencia del sistema distribuido:
 *    - `KafkaProducerService`: Publicación con clave customerId y soporte DLQ.
 *    - `IdempotencyService`: Verificación y deduplicación atómica de eventos recibidos.
 *    - `DlqRetryService`: Lógica de 3 reintentos (1s, 2s, 4s) y desvío a <topic>.dlq.
 *    - `OutboxService`: Almacenamiento seguro y worker en segundo plano para publicación confiable.
 * =================================================================================================
 */

import { Module, DynamicModule } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ProcessedEvent, ProcessedEventSchema } from './idempotency/processed-event.schema';
import { OutboxRecord, OutboxSchema } from './outbox/outbox.schema';
import { IdempotencyService } from './idempotency/idempotency.service';
import { DlqRetryService } from './dlq-retry/dlq-retry.service';
import { OutboxService } from './outbox/outbox.service';
import { KafkaProducerService } from './kafka-producer/kafka-producer.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
      { name: OutboxRecord.name, schema: OutboxSchema },
    ]),
  ],
  providers: [
    KafkaProducerService,
    IdempotencyService,
    DlqRetryService,
    OutboxService,
  ],
  exports: [
    KafkaProducerService,
    IdempotencyService,
    DlqRetryService,
    OutboxService,
    MongooseModule,
  ],
})
export class KafkaToolkitModule {
  static register(): DynamicModule {
    return {
      module: KafkaToolkitModule,
      imports: [
        MongooseModule.forFeature([
          { name: ProcessedEvent.name, schema: ProcessedEventSchema },
          { name: OutboxRecord.name, schema: OutboxSchema },
        ]),
      ],
      providers: [
        KafkaProducerService,
        IdempotencyService,
        DlqRetryService,
        OutboxService,
      ],
      exports: [
        KafkaProducerService,
        IdempotencyService,
        DlqRetryService,
        OutboxService,
        MongooseModule,
      ],
    };
  }
}
