/**
 * =================================================================================================
 * ARCHIVO: apps/crm-analytics-service/src/schemas/event-log.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del modelo y esquema Mongoose para la colección `event_log` en `crm_db` (Sección 07).
 * 2. Almacena la copia inmutable de todos los eventos del ecosistema Kafka para auditoría y analítica.
 * 3. Atributos incluidos:
 *    - `eventId`: Identificador único del evento con índice único.
 *    - `correlationId`: Identificador de la saga/activación con índice para reconstruir trazabilidad de punta a punta.
 *    - `customerId`: Identificador del cliente.
 *    - `eventType`: Nombre del evento registrado.
 *    - `source`: Servicio que originó el evento.
 *    - `payload`: Payload íntegro del evento.
 *    - `occurredAt`: Fecha y hora de ocurrencia original.
 *    - `receivedAt`: Fecha y hora de recepción por parte del consumidor de analytics.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type EventLogDocument = EventLog & Document;

@Schema({ collection: 'event_log', timestamps: true })
export class EventLog {
  @Prop({ required: true, unique: true, index: true })
  eventId: string;

  @Prop({ required: true, index: true })
  correlationId: string;

  @Prop({ required: true, index: true })
  customerId: string;

  @Prop({ required: true, index: true })
  eventType: string;

  @Prop({ required: true })
  source: string;

  @Prop({ required: true, type: Object })
  payload: Record<string, any>;

  @Prop({ required: true })
  occurredAt: string;

  @Prop({ default: Date.now })
  receivedAt: Date;
}

export const EventLogSchema = SchemaFactory.createForClass(EventLog);
