/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/kafka-producer/kafka-producer.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Implementación del servicio productor de Apache Kafka para publicación confiable de eventos.
 * 2. Garantiza el cumplimiento de RNF-02:
 *    - La clave del mensaje (message key) SIEMPRE es el `customerId` (clave de partición).
 *    - Esto asegura que todos los eventos de un mismo cliente caigan siempre en la misma partición
 *      de Kafka y sean leídos en orden estricto secuencial.
 * 3. Provee métodos:
 *    - `publishDirect`: Publica un sobre EventEnvelope serializado en JSON al tópico indicado con key=customerId.
 *    - `sendToDlq`: Publica mensajes que fallaron tras agotar los 3 reintentos al tópico DLQ (<topic>.dlq)
 *      incluyendo headers de diagnóstico técnico.
 * 4. Gestiona el ciclo de vida del productor (conexión onModuleInit y desconexión onModuleDestroy).
 * =================================================================================================
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Producer, RecordMetadata } from 'kafkajs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private kafka: Kafka;
  private producer: Producer;
  private isConnected = false;

  constructor() {
    const brokers = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
    const clientId = process.env.KAFKA_CLIENT_ID || 'poc-producer';

    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 300,
        retries: 8,
      },
    });

    this.producer = this.kafka.producer({
      allowAutoTopicCreation: true,
      idempotent: false,
    });
  }

  async onModuleInit() {
    await this.connectWithRetry();
  }

  async onModuleDestroy() {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
    }
  }

  private async connectWithRetry(retries = 10, delay = 2000): Promise<void> {
    for (let i = 1; i <= retries; i++) {
      try {
        await this.producer.connect();
        this.isConnected = true;
        this.logger.log(`[KAFKA PRODUCER] Conectado exitosamente al broker Kafka`);
        return;
      } catch (err: any) {
        this.logger.warn(
          `[KAFKA PRODUCER] Intento de conexión ${i}/${retries} falló: ${err?.message}. Reintentando en ${delay}ms...`,
        );
        if (i === retries) {
          this.logger.error(`[KAFKA PRODUCER CRITICAL] No se pudo conectar con Kafka: ${err?.message}`);
          return;
        }
        await new Promise((res) => setTimeout(res, delay));
      }
    }
  }

  /**
   * Publica directamente un evento a un tópico garantizando la clave de partición por customerId.
   */
  async publishDirect(
    topic: string,
    partitionKey: string,
    envelope: any,
  ): Promise<RecordMetadata[]> {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
    }

    const value = Buffer.from(JSON.stringify(envelope));

    const metadata = await this.producer.send({
      topic,
      messages: [
        {
          key: partitionKey,
          value,
          headers: {
            'x-correlation-id': envelope.correlationId || '',
            'x-event-type': envelope.eventType || '',
            'x-source': envelope.source || '',
          },
        },
      ],
    });

    this.logger.log(
      `[EVENTO PUBLICADO] Tópico: ${topic} | Clave(customerId): ${partitionKey} | eventType: ${envelope.eventType} | correlationId: ${envelope.correlationId}`,
    );

    return metadata;
  }

  /**
   * Envía un mensaje agotado a la Dead Letter Queue (<topic>.dlq) con headers de error.
   */
  async sendToDlq(
    dlqTopic: string,
    partitionKey: string,
    envelope: any,
    headers: Record<string, string>,
  ): Promise<RecordMetadata[]> {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
    }

    const kafkaHeaders: Record<string, string> = { ...headers };
    if (envelope?.correlationId) kafkaHeaders['x-correlation-id'] = envelope.correlationId;
    if (envelope?.eventType) kafkaHeaders['x-event-type'] = envelope.eventType;

    const metadata = await this.producer.send({
      topic: dlqTopic,
      messages: [
        {
          key: partitionKey,
          value: Buffer.from(JSON.stringify(envelope)),
          headers: kafkaHeaders,
        },
      ],
    });

    this.logger.warn(
      `[DLQ PUBLICADO] Mensaje enviado a ${dlqTopic} | Clave: ${partitionKey} | Error: ${headers['x-error-message']}`,
    );

    return metadata;
  }
}
