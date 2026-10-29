/**
 * =================================================================================================
 * ARCHIVO: apps/billing-service/src/main.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Inicialización y arranque del microservicio NestJS `billing-service`.
 * 2. Conexión como consumidor de Kafka en el grupo `billing-svc` (CONSUMER_GROUPS.BILLING_SERVICE).
 * 3. Habilita auto-rebalanceo y particionamiento por `customerId` (RNF-02).
 * 4. Soporta escalamiento horizontal de múltiples instancias (`docker compose up --scale billing-service=2`).
 * =================================================================================================
 */

import { NestFactory } from '@nestjs/core';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { BillingModule } from './billing.module';

async function bootstrap() {
  const logger = new Logger('BillingMain');
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const instanceId = process.env.HOSTNAME || `billing-${Math.random().toString(36).substring(7)}`;

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    BillingModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `billing-service-${instanceId}`,
          brokers,
          retry: { retries: 10, initialRetryTime: 500 },
        },
        consumer: {
          groupId: CONSUMER_GROUPS.BILLING_SERVICE,
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
  logger.log(`🚀 billing-service conectado a Kafka (Instancia: ${instanceId})`);
  logger.log(`📬 Consumer Group: ${CONSUMER_GROUPS.BILLING_SERVICE}`);
  logger.log(`=======================================================`);
}

bootstrap().catch((err) => {
  console.error('Error fatal al iniciar billing-service:', err);
  process.exit(1);
});
