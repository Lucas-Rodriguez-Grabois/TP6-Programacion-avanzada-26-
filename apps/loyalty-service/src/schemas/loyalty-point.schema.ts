/**
 * =================================================================================================
 * ARCHIVO: apps/loyalty-service/src/schemas/loyalty-point.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del modelo y esquema Mongoose para la colección `loyalty_points` en `loyalty_db`.
 * 2. Guarda el saldo acumulado de puntos de recompensa por cliente generado durante el replay histórico.
 * 3. Atributos incluidos:
 *    - `customerId`: Clave única por cliente con índice.
 *    - `points`: Saldo total de puntos acumulados.
 *    - `eventsProcessed`: Cantidad de eventos históricos que aportaron puntos.
 *    - `lastAwardedAt`: Marca temporal de la última asignación de puntos.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type LoyaltyPointDocument = LoyaltyPoint & Document;

@Schema({ collection: 'loyalty_points', timestamps: true })
export class LoyaltyPoint {
  @Prop({ required: true, unique: true, index: true })
  customerId: string;

  @Prop({ required: true, default: 0 })
  points: number;

  @Prop({ required: true, default: 0 })
  eventsProcessed: number;

  @Prop({ default: Date.now })
  lastAwardedAt: Date;
}

export const LoyaltyPointSchema = SchemaFactory.createForClass(LoyaltyPoint);
