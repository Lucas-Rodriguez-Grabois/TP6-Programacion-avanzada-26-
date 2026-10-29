/**
 * =================================================================================================
 * ARCHIVO: apps/crm-analytics-service/src/main.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Inicialización y arranque del microservicio NestJS `crm-analytics-service`.
 * 2. Conexión como consumidor de Kafka en el grupo `crm-analytics` (CONSUMER_GROUPS.CRM_ANALYTICS).
 * 3. Habilita lectura desde todos los tópicos del cluster de forma totalmente desacoplada.
 * =================================================================================================
 */

import { NestFactory } from '@nestjs/core';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { CrmModule } from './crm.module';

async function bootstrap() {
  const logger = new Logger('CrmAnalyticsMain');
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const instanceId = process.env.HOSTNAME || `crm-${Math.random().toString(36).substring(7)}`;

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    CrmModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `crm-analytics-${instanceId}`,
          brokers,
          retry: { retries: 10, initialRetryTime: 500 },
        },
        consumer: {
          groupId: CONSUMER_GROUPS.CRM_ANALYTICS,
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
  logger.log(`🚀 crm-analytics-service conectado a Kafka (Instancia: ${instanceId})`);
  logger.log(`📬 Consumer Group: ${CONSUMER_GROUPS.CRM_ANALYTICS}`);
  logger.log(`=======================================================`);
}

bootstrap().catch((err) => {
  console.error('Error fatal al iniciar crm-analytics-service:', err);
  process.exit(1);
});
