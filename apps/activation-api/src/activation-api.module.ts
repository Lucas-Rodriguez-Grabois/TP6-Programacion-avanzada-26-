/**
 * =================================================================================================
 * ARCHIVO: apps/activation-api/src/activation-api.module.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del módulo raíz de NestJS para la aplicación `activation-api`.
 * 2. Configura la conexión a MongoDB utilizando la variable de entorno `MONGO_URI`
 *    (apuntando a la base de datos aislada `activation_db`).
 * 3. Registra el esquema `ActivationSchema` en Mongoose.
 * 4. Importa la librería compartida `KafkaToolkitModule` para soporte de idempotencia, DLQ y outbox.
 * 5. Registra el controlador REST `ActivationsController`, el servicio `ActivationsService`
 *    y la pasarela de WebSockets `ActivationGateway`.
 * =================================================================================================
 */

import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { Activation, ActivationSchema } from './activations/activation.schema';
import { ActivationsController } from './activations/activations.controller';
import { ActivationsService } from './activations/activations.service';
import { ActivationGateway } from './gateway/activation.gateway';

const mongoUri =
  process.env.MONGO_URI ||
  'mongodb://localhost:27017/activation_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: Activation.name, schema: ActivationSchema },
    ]),
    KafkaToolkitModule,
  ],
  controllers: [ActivationsController],
  providers: [ActivationsService, ActivationGateway],
  exports: [ActivationsService],
})
export class ActivationApiModule {}
