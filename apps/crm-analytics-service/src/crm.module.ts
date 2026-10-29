/**
 * =================================================================================================
 * ARCHIVO: apps/crm-analytics-service/src/crm.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Módulo principal de NestJS para `crm-analytics-service`.
 * 2. Conexión aislada a MongoDB en la base de datos `crm_db`.
 * 3. Registra el esquema `EventLog` para la bitácora inmutable de eventos.
 * 4. Importa `KafkaToolkitModule` para soporte de idempotencia y DLQ.
 * 5. Registra `CrmController` y `CrmService`.
 * =================================================================================================
 */

import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { EventLog, EventLogSchema } from './schemas/event-log.schema';
import { CrmController } from './crm.controller';
import { CrmService } from './crm.service';

const mongoUri =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/crm_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: EventLog.name, schema: EventLogSchema },
    ]),
    KafkaToolkitModule,
  ],
  controllers: [CrmController],
  providers: [CrmService],
})
export class CrmModule {}
