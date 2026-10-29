/**
 * =================================================================================================
 * ARCHIVO: apps/activation-api/src/main.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Inicialización y arranque de `activation-api` como aplicación híbrida (HTTP REST + Microservicio Kafka).
 * 2. Configura el servidor HTTP en el puerto 3000 con soporte de CORS abierto para la Demo UI (puerto 5173).
 * 3. Conecta el microservicio Kafka utilizando el transporte oficial `@nestjs/microservices`:
 *    - Brokers leídos desde `process.env.KAFKA_BROKERS` (ej: 'kafka:9092' o 'localhost:9092').
 *    - Consumer Group fijado en `CONSUMER_GROUPS.ACTIVATION_API` ('activation-api').
 * 4. Inicia la escucha de eventos y peticiones REST con logging descriptivo.
 * =================================================================================================
 */

import { NestFactory } from '@nestjs/core';
import { Transport, MicroserviceOptions } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { CONSUMER_GROUPS } from '@poc/contracts';
import { ActivationApiModule } from './activation-api.module';

async function bootstrap() {
  const logger = new Logger('ActivationApiMain');
  const app = await NestFactory.create(ActivationApiModule);

  // Habilitar CORS para permitir llamadas desde el frontend en React
  app.enableCors({
    origin: '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
  });

  const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

  // Conexión híbrida como consumidor Kafka
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'activation-api-consumer',
        brokers,
        retry: { retries: 10, initialRetryTime: 500 },
      },
      consumer: {
        groupId: CONSUMER_GROUPS.ACTIVATION_API,
        sessionTimeout: 10000,
        heartbeatInterval: 3000,
      },
      subscribe: {
        fromBeginning: false,
      },
    },
  });

  await app.startAllMicroservices();
  const port = process.env.PORT || 3000;
  await app.listen(port);

  logger.log(`=======================================================`);
  logger.log(`🚀 activation-api corriendo exitosamente en http://localhost:${port}`);
  logger.log(`📡 WebSocket Gateway activo para timeline en vivo`);
  logger.log(`📬 Consumiendo tópicos Kafka con grupo: ${CONSUMER_GROUPS.ACTIVATION_API}`);
  logger.log(`=======================================================`);
}

bootstrap().catch((err) => {
  console.error('Error fatal al iniciar activation-api:', err);
  process.exit(1);
});
