/**
 * =================================================================================================
 * ARCHIVO: apps/notification-service/src/main.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Inicialización y arranque del microservicio NestJS `notification-service`.
 * 2. Conexión como consumidor de Kafka en el grupo `notification-svc` (CONSUMER_GROUPS.NOTIFICATION_SERVICE).
 * 3. Demuestra la resiliencia de offsets de Kafka (Escenario 3 del Guión de Demo):
 *    - Si el servicio se apaga (`docker compose stop notification-service`), los mensajes se acumulan
 *      en Kafka sin perderse (lag observable en Kafka UI).
 *    - Al reiniciarse (`docker compose start notification-service`), procesa todos los eventos pendientes
 *      desde su último offset confirmado (committed offset).
 * =================================================================================================
 */

import { NestFactory } from '@nestjs/core';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { NotificationModule } from './notification.module';

async function bootstrap() {
  const logger = new Logger('NotificationMain');
  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
  const instanceId = process.env.HOSTNAME || `notification-${Math.random().toString(36).substring(7)}`;

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    NotificationModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `notification-service-${instanceId}`,
          brokers,
          retry: { retries: 10, initialRetryTime: 500 },
        },
        consumer: {
          groupId: CONSUMER_GROUPS.NOTIFICATION_SERVICE,
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
  logger.log(`🚀 notification-service conectado a Kafka (Instancia: ${instanceId})`);
  logger.log(`📬 Consumer Group: ${CONSUMER_GROUPS.NOTIFICATION_SERVICE}`);
  logger.log(`=======================================================`);
}

bootstrap().catch((err) => {
  console.error('Error fatal al iniciar notification-service:', err);
  process.exit(1);
});
