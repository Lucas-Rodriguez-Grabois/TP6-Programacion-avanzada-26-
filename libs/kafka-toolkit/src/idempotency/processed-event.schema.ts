/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/idempotency/processed-event.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del esquema de Mongoose para la colección `processed_events`.
 * 2. Implementación de los índices requeridos por la arquitectura (Sección 07 y 08):
 *    - Índice único compuesto o en eventId para detectar eventos duplicados por consumidor.
 *    - Índice TTL de 7 días sobre `expireAt` para purga automática, coincidente con la retención
 *      de los tópicos de Kafka (evita crecimiento indefinido de la base de datos).
 * 3. Provee la estructura base para garantizar la semántica "exactly-once processing" a nivel de aplicación.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type ProcessedEventDocument = ProcessedEvent & Document;

@Schema({ collection: 'processed_events', timestamps: true })
export class ProcessedEvent {
  @Prop({ required: true, index: true })
  eventId: string;

  @Prop({ required: true })
  consumerGroup: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  correlationId: string;

  @Prop({ required: true, default: Date.now })
  processedAt: Date;

  @Prop({
    required: true,
    default: () => new Date(Date.now() + 7 * 24 * 60 * 60 * 1000), // 7 días de retención
    index: { expireAfterSeconds: 0 },
  })
  expireAt: Date;
}

export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);
// Índice único por combinación de eventId y consumerGroup para permitir múltiples grupos
ProcessedEventSchema.index({ eventId: 1, consumerGroup: 1 }, { unique: true });
