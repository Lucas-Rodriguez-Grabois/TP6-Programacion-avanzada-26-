/**
 * =================================================================================================
 * ARCHIVO: apps/billing-service/src/schemas/billing-account.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del modelo Mongoose para la colección `billing_accounts` en `billing_db`.
 * 2. Representa la cuenta de facturación simulada generada por la contratación del plan (Sección 07).
 * 3. Campos requeridos:
 *    - `activationId`: Identificador único de la activación con índice único.
 *    - `customerId`: Cliente asociado a la cuenta de facturación.
 *    - `status`: Estado de la cuenta, 'CREATED' o 'CANCELLED' (cuando opera la compensación de la saga).
 *    - `monthlyFee`: Cuota mensual simulada del plan.
 *    - `cancellationReason`: Motivo registrado en caso de compensación / anulación.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type BillingAccountDocument = BillingAccount & Document;

@Schema({ collection: 'billing_accounts', timestamps: true })
export class BillingAccount {
  @Prop({ required: true, unique: true, index: true })
  activationId: string;

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({
    required: true,
    enum: ['CREATED', 'CANCELLED'],
    default: 'CREATED',
  })
  status: 'CREATED' | 'CANCELLED';

  @Prop({ required: true, default: 4500 })
  monthlyFee: number;

  @Prop({ required: false })
  cancellationReason?: string;

  @Prop({ default: Date.now })
  createdAt: Date;

  @Prop({ default: Date.now })
  updatedAt: Date;
}

export const BillingAccountSchema = SchemaFactory.createForClass(BillingAccount);
