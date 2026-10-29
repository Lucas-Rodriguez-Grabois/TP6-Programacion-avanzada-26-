/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/outbox/outbox.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Implementación del Patrón Transactional Outbox (Decisión Técnica 3 y Fase 4).
 * 2. Guarda los eventos en la colección `outbox` dentro de la misma sesión o transacción de MongoDB
 *    que los cambios de la base de datos de negocio.
 * 3. Proceso en segundo plano (Outbox Relay Worker) que corre periódicamente cada 500 ms:
 *    - Lee mensajes en estado 'PENDING'.
 *    - Los publica de forma confiable a Apache Kafka utilizando `KafkaProducerService` con partitionKey = customerId.
 *    - Actualiza el estado a 'PUBLISHED' con su marca de tiempo `publishedAt`.
 * 4. Evita inconsistencias dual-write ante caídas del servidor entre el guardado en base de datos
 *    y la publicación al broker Kafka.
 * =================================================================================================
 */

import { Injectable, Logger, OnModuleInit, OnModuleDestroy, forwardRef, Inject } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, ClientSession } from 'mongoose';
import { OutboxRecord, OutboxDocument } from './outbox.schema';
import { KafkaProducerService } from '../kafka-producer/kafka-producer.service';

@Injectable()
export class OutboxService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OutboxService.name);
  private timer: NodeJS.Timeout | null = null;
  private isProcessing = false;

  constructor(
    @InjectModel(OutboxRecord.name)
    private readonly outboxModel: Model<OutboxDocument>,
    @Inject(forwardRef(() => KafkaProducerService))
    private readonly kafkaProducer: KafkaProducerService,
  ) {}

  onModuleInit() {
    this.startWorker();
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Guarda un evento pendiente en la colección outbox.
   * Si se pasa un `session`, se ejecuta dentro de la transacción de negocio.
   */
  async saveEvent(
    topic: string,
    partitionKey: string,
    payload: Record<string, any>,
    session?: ClientSession,
  ): Promise<OutboxDocument> {
    const record = new this.outboxModel({
      topic,
      partitionKey,
      payload,
      status: 'PENDING',
      createdAt: new Date(),
    });

    if (session) {
      return await record.save({ session });
    }
    return await record.save();
  }

  /**
   * Inicia el ciclo de polling del relay cada 500 ms según la especificación del PDF.
   */
  private startWorker() {
    this.timer = setInterval(async () => {
      await this.processPendingMessages();
    }, 500);
  }

  /**
   * Procesa los registros pendientes de la outbox y los emite a Kafka.
   */
  async processPendingMessages(): Promise<number> {
    if (this.isProcessing) return 0;
    this.isProcessing = true;

    try {
      const pendings = await this.outboxModel
        .find({ status: 'PENDING' })
        .sort({ createdAt: 1 })
        .limit(20)
        .exec();

      if (!pendings || pendings.length === 0) {
        this.isProcessing = false;
        return 0;
      }

      for (const record of pendings) {
        try {
          await this.kafkaProducer.publishDirect(
            record.topic,
            record.partitionKey,
            record.payload,
          );

          record.status = 'PUBLISHED';
          record.publishedAt = new Date();
          await record.save();
        } catch (err: any) {
          this.logger.error(
            `[OUTBOX RELAY] Error publicando registro ${record._id} a tópico ${record.topic}: ${err?.message}`,
          );
          record.status = 'FAILED';
          record.errorMessage = err?.message || String(err);
          await record.save();
        }
      }

      return pendings.length;
    } catch (e: any) {
      this.logger.error(`[OUTBOX RELAY ERROR] ${e?.message}`);
      return 0;
    } finally {
      this.isProcessing = false;
    }
  }
}
