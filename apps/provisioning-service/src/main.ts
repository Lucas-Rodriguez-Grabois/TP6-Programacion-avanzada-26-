/**
 * =================================================================================================
 * ARCHIVO: apps/provisioning-service/src/main.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Inicialización y arranque del microservicio NestJS `provisioning-service`.
 * 2. Conexión como consumidor de Kafka en el grupo `provisioning-svc` (CONSUMER_GROUPS.PROVISIONING_SERVICE).
 * 3. Habilita el procesamiento distribuido de eventos en paralelo con `billing-service` (RF-04).
 * =================================================================================================
 */

import { NestFactory } from '@nestjs/core';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ProvisioningModule } from './provisioning.module';

async function bootstrap() {
  const logger = new Logger('ProvisioningMain');
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const instanceId = process.env.HOSTNAME || `provisioning-${Math.random().toString(36).substring(7)}`;

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    ProvisioningModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `provisioning-service-${instanceId}`,
          brokers,
          retry: { retries: 10, initialRetryTime: 500 },
        },
        consumer: {
          groupId: CONSUMER_GROUPS.PROVISIONING_SERVICE,
          sessionTimeout: 10000,
          heartbeatInterval: 3000,
        },
        subscribe: {
          fromBeginning: false,
        },
      },
    },
  );

  await app.listen();
  logger.log(`=======================================================`);
  logger.log(`🚀 provisioning-service conectado a Kafka (Instancia: ${instanceId})`);
  logger.log(`📬 Consumer Group: ${CONSUMER_GROUPS.PROVISIONING_SERVICE}`);
  logger.log(`=======================================================`);
}

bootstrap().catch((err) => {
  console.error('Error fatal al iniciar provisioning-service:', err);
  process.exit(1);
});
