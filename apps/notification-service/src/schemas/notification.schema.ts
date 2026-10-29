/**
 * =================================================================================================
 * ARCHIVO: apps/notification-service/src/schemas/notification.schema.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del modelo Mongoose para la colección `notifications` (Sección 07 del PDF).
 * 2. Almacena el registro de emails enviados a través de Mailhog.
 * 3. Atributos incluidos:
 *    - `activationId`: Identificador de la activación.
 *    - `eventType`: Tipo de evento que disparó la notificación (ActivationCompleted o ActivationFailed).
 *    - `recipient`: Dirección de correo electrónico del cliente.
 *    - `subject`: Asunto del correo.
 *    - `body`: Cuerpo del correo electrónico en formato HTML/texto.
 *    - `status`: Estado de envío ('SENT' | 'FAILED').
 *    - `sentAt`: Fecha y hora de envío.
 * 4. Índice compuesto sobre { activationId: 1, eventType: 1 } según la especificación.
 * =================================================================================================
 */

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type NotificationDocument = NotificationRecord & Document;

@Schema({ collection: 'notifications', timestamps: true })
export class NotificationRecord {
  @Prop({ required: true, index: true })
  activationId: string;

  @Prop({ required: true })
  eventType: string;

  @Prop({ required: true })
  recipient: string;

  @Prop({ required: true })
  subject: string;

  @Prop({ required: true })
  body: string;

  @Prop({ required: true, enum: ['SENT', 'FAILED'], default: 'SENT' })
  status: 'SENT' | 'FAILED';

  @Prop({ default: Date.now })
  sentAt: Date;
}

export const NotificationSchema = SchemaFactory.createForClass(NotificationRecord);
NotificationSchema.index({ activationId: 1, eventType: 1 });
