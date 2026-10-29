/**
 * =================================================================================================
 * ARCHIVO: apps/loyalty-service/src/loyalty.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Módulo principal de NestJS para `loyalty-service`.
 * 2. Conecta con la base de datos `loyalty_db` en MongoDB.
 * 3. Registra el esquema `LoyaltyPoint` para el balance acumulado de puntos.
 * 4. Importa `KafkaToolkitModule` para soporte de idempotencia y DLQ.
 * 5. Registra `LoyaltyController` y `LoyaltyService`.
 * =================================================================================================
 */

import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { LoyaltyPoint, LoyaltyPointSchema } from './schemas/loyalty-point.schema';
import { LoyaltyController } from './loyalty.controller';
import { LoyaltyService } from './loyalty.service';

const mongoUri =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/loyalty_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: LoyaltyPoint.name, schema: LoyaltyPointSchema },
    ]),
    KafkaToolkitModule,
  ],
  controllers: [LoyaltyController],
  providers: [LoyaltyService],
})
export class LoyaltyModule {}
