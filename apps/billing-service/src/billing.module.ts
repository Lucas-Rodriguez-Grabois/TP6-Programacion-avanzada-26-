/**
 * =================================================================================================
 * ARCHIVO: apps/billing-service/src/billing.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Módulo raíz de NestJS para `billing-service`.
 * 2. Conexión aislada a MongoDB en la base de datos `billing_db`.
 * 3. Registro del esquema `BillingAccount` para persistencia de cuentas de facturación simuladas.
 * 4. Integración del `KafkaToolkitModule` para soporte de idempotencia, reintentos, DLQ y outbox.
 * 5. Registro de controladores y servicios de facturación.
 * =================================================================================================
 */

import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { BillingAccount, BillingAccountSchema } from './schemas/billing-account.schema';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';

const mongoUri =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/billing_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: BillingAccount.name, schema: BillingAccountSchema },
    ]),
    KafkaToolkitModule,
  ],
  controllers: [BillingController],
  providers: [BillingService],
})
export class BillingModule {}
