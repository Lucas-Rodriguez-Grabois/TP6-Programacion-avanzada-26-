/**
 * =================================================================================================
 * ARCHIVO: apps/provisioning-service/src/schemas/provisioning-order.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del modelo y esquema Mongoose para la colección `provisioning_orders` (Sección 07).
 * 2. Representa la orden de configuración técnica y asignación de circuito en la red de telecomunicaciones.
 * 3. Atributos incluidos:
 *    - `activationId`: Identificador único de la activación con índice único.
 *    - `customerId`: Cliente asociado.
 *    - `orderId`: Número de orden de aprovisionamiento de red.
 *    - `circuitId`: Identificador del circuito físico/lógico asignado.
 *    - `status`: Estado de la orden ('COMPLETED' | 'FAILED').
 *    - `completedAt`: Marca temporal de finalización técnica.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type ProvisioningOrderDocument = ProvisioningOrder & Document;

@Schema({ collection: 'provisioning_orders', timestamps: true })
export class ProvisioningOrder {
  @Prop({ required: true, unique: true, index: true })
  activationId: string;

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({ required: true })
  orderId: string;

  @Prop({ required: true })
  circuitId: string;

  @Prop({ required: true, enum: ['COMPLETED', 'FAILED'], default: 'COMPLETED' })
  status: 'COMPLETED' | 'FAILED';

  @Prop({ required: false })
  reason?: string;

  @Prop({ default: Date.now })
  completedAt: Date;

  @Prop({ default: Date.now })
  createdAt: Date;
}

export const ProvisioningOrderSchema = SchemaFactory.createForClass(ProvisioningOrder);
