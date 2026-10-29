/**
 * =================================================================================================
 * ARCHIVO: apps/notification-service/src/notification.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Módulo raíz de NestJS para `notification-service`.
 * 2. Conecta con la base de datos `notification_db` en MongoDB.
 * 3. Registra el esquema `NotificationRecord` para trazabilidad de notificaciones enviadas.
 * 4. Importa `KafkaToolkitModule` para soporte de deduplicación idempotente y DLQ.
 * 5. Registra el controlador `NotificationController` y el servicio `NotificationService`.
 * =================================================================================================
 */

import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import {
  NotificationRecord,
  NotificationSchema,
} from './schemas/notification.schema';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';

const mongoUri =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/notification_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: NotificationRecord.name, schema: NotificationSchema },
    ]),
    KafkaToolkitModule,
  ],
  controllers: [NotificationController],
  providers: [NotificationService],
})
export class NotificationModule {}
