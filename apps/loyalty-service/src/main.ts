/**
 * =================================================================================================
 * ARCHIVO: apps/loyalty-service/src/main.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Inicialización y arranque del microservicio NestJS `loyalty-service` (Escenario 5 del Guión de Demo).
 * 2. Conexión como consumidor de Kafka en el grupo `loyalty-group` (CONSUMER_GROUPS.LOYALTY_SERVICE).
 * 3. Configuración crítica de `fromBeginning: true` (`auto.offset.reset = 'earliest'`):
 *    - Al arrancar este servicio con un grupo nuevo, Kafka lee todo el log histórico de eventos desde
 *      el inicio de las particiones.
 *    - Demuestra una de las propiedades más poderosas de Apache Kafka: El replay histórico sin
 *      afectar ni detener a ningún otro servicio en ejecución.
 * =================================================================================================
 */

import { NestFactory } from '@nestjs/core';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { LoyaltyModule } from './loyalty.module';

async function bootstrap() {
  const logger = new Logger('LoyaltyMain');
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const instanceId = process.env.HOSTNAME || `loyalty-${Math.random().toString(36).substring(7)}`;

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    LoyaltyModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `loyalty-service-${instanceId}`,
          brokers,
          retry: { retries: 10, initialRetryTime: 500 },
        },
        consumer: {
          groupId: CONSUMER_GROUPS.LOYALTY_SERVICE,
          sessionTimeout: 10000,
          heartbeatInterval: 3000,
        },
        subscribe: {
          // auto.offset.reset: 'earliest' para releer todo el historial
          fromBeginning: true,
        },
      },
    },
  );

  await app.listen();
  logger.log(`=======================================================`);
  logger.log(`🚀 loyalty-service conectado a Kafka (Instancia: ${instanceId})`);
  logger.log(`📬 Consumer Group: ${CONSUMER_GROUPS.LOYALTY_SERVICE}`);
  logger.log(`⏮️  Modo Replay activado: fromBeginning = true (auto.offset.reset = earliest)`);
  logger.log(`=======================================================`);
}

bootstrap().catch((err) => {
  console.error('Error fatal al iniciar loyalty-service:', err);
  process.exit(1);
});
