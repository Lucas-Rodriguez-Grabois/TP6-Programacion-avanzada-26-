/**
 * =================================================================================================
 * ARCHIVO: apps/provisioning-service/src/provisioning.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del módulo NestJS para `provisioning-service`.
 * 2. Conecta con la base de datos `provisioning_db` en MongoDB.
 * 3. Registra el esquema `ProvisioningOrder` para el almacenamiento de órdenes de aprovisionamiento.
 * 4. Importa `KafkaToolkitModule` para soporte de idempotencia, reintentos, DLQ y outbox.
 * 5. Registra el controlador `ProvisioningController` y el servicio `ProvisioningService`.
 * =================================================================================================
 */

import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import {
  ProvisioningOrder,
  ProvisioningOrderSchema,
} from './schemas/provisioning-order.schema';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';

const mongoUri =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/provisioning_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: ProvisioningOrder.name, schema: ProvisioningOrderSchema },
    ]),
    KafkaToolkitModule,
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
export class ProvisioningModule {}
