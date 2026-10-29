/**
 * =================================================================================================
 * ARCHIVO: apps/activation-api/src/activations/activation.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del modelo y esquema Mongoose para la colección `activations` (Sección 07 del PDF).
 * 2. Implementación de los atributos clave del agregado de la Saga:
 *    - `_id`: activationId personalizado (ej. `act-xxxx`) como clave primaria.
 *    - `customerId`: Identificador del cliente con índice para consultas rápidas.
 *    - `planId`: Nombre o ID del plan contratado.
 *    - `status`: Estado actual de la saga ('PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED').
 *    - `simulateFailure`: Modo de prueba inyectado ('none' | 'billing' | 'provisioning').
 *    - `steps`: Diccionario con el resultado individual de billing y provisioning.
 *    - `history`: Arreglo cronológico de eventos registrados a lo largo del ciclo de vida.
 *    - `createdAt` y `updatedAt`: Marcas de auditoría temporal.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

export interface StepResult {
  status: 'OK' | 'FAILED';
  at: string;
  details?: Record<string, any>;
  reason?: string;
}

export interface HistoryItem {
  eventType: string;
  at: string;
  source?: string;
  details?: Record<string, any>;
}

export type ActivationDocument = Activation & Document;

@Schema({ collection: 'activations', timestamps: true })
export class Activation {
  @Prop({ required: true })
  _id: string; // activationId, ej: "act-0001"

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({ required: true })
  planId: string;

  @Prop({
    required: true,
    enum: ['PENDING', 'IN_PROGRESS', 'ACTIVE', 'FAILED'],
    default: 'PENDING',
    index: true,
  })
  status: ActivationStatus;

  @Prop({
    required: true,
    enum: ['none', 'billing', 'provisioning'],
    default: 'none',
  })
  simulateFailure: 'none' | 'billing' | 'provisioning';

  @Prop({
    type: Object,
    default: () => ({ billing: null, provisioning: null }),
  })
  steps: {
    billing?: StepResult | null;
    provisioning?: StepResult | null;
  };

  @Prop({ type: Array, default: [] })
  history: HistoryItem[];

  @Prop({ default: Date.now })
  createdAt: Date;

  @Prop({ default: Date.now })
  updatedAt: Date;
}

export const ActivationSchema = SchemaFactory.createForClass(Activation);
ActivationSchema.index({ customerId: 1 });
