/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/outbox/outbox.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del esquema de Mongoose para la colección `outbox` (Sección 07 y Decisión 3).
 * 2. Estructura de almacenamiento transaccional para garantizar que la mutación de estado de negocio
 *    y la emisión del evento en Kafka ocurran de forma atómica dentro de una misma transacción MongoDB.
 * 3. Campos incluidos:
 *    - topic: Tópico destino de Kafka.
 *    - partitionKey: Clave del mensaje (customerId) para orden de partición.
 *    - payload: Contenido completo del sobre EventEnvelope serializado o como subdocumento.
 *    - status: 'PENDING' | 'PUBLISHED' | 'FAILED'.
 *    - createdAt y publishedAt: Marcas de tiempo de ciclo de vida.
 *    - Índices: Índice compuesto sobre { status: 1, createdAt: 1 } para consulta ultra-rápida del worker.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type OutboxDocument = OutboxRecord & Document;

@Schema({ collection: 'outbox', timestamps: true })
export class OutboxRecord {
  @Prop({ required: true, index: true })
  topic: string;

  @Prop({ required: true })
  partitionKey: string;

  @Prop({ required: true, type: Object })
  payload: Record<string, any>;

  @Prop({ required: true, enum: ['PENDING', 'PUBLISHED', 'FAILED'], default: 'PENDING', index: true })
  status: 'PENDING' | 'PUBLISHED' | 'FAILED';

  @Prop({ required: false })
  errorMessage?: string;

  @Prop({ required: false })
  publishedAt?: Date;

  @Prop({ default: Date.now })
  createdAt: Date;
}

export const OutboxSchema = SchemaFactory.createForClass(OutboxRecord);
OutboxSchema.index({ status: 1, createdAt: 1 });
