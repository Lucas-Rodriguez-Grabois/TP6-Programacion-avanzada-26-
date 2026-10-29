/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/idempotency/idempotency.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Implementación del servicio de Idempotencia para consumidores Kafka (RNF-03 y Decisión 1).
 * 2. Kafka provee garantía "at least once": ante caídas de consumidores antes de confirmar el commit
 *    del offset, el mensaje se reentrega.
 * 3. Este servicio intercepta la ejecución:
 *    - Verifica e inserta atómicamente el `eventId` y `consumerGroup` en la colección `processed_events`.
 *    - Si MongoDB arroja error E11000 (clave duplicada), se detecta inmediatamente que el evento
 *      ya fue ejecutado previamente, descartándolo sin duplicar efectos en el negocio.
 *    - Provee soporte para sesiones transaccionales de MongoDB (`ClientSession`).
 * =================================================================================================
 */

import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model, ClientSession } from 'mongoose';
import { ProcessedEvent, ProcessedEventDocument } from './processed-event.schema';

@Injectable()
export class IdempotencyService {
  private readonly logger = new Logger(IdempotencyService.name);

  constructor(
    @InjectModel(ProcessedEvent.name)
    private readonly processedEventModel: Model<ProcessedEventDocument>,
  ) {}

  /**
   * Intenta registrar el evento como procesado.
   * Retorna true si es la primera vez que se procesa (éxito).
   * Retorna false si ya fue procesado con anterioridad (duplicado detectado).
   */
  async tryProcess(
    eventId: string,
    consumerGroup: string,
    eventType: string,
    correlationId: string,
    session?: ClientSession,
  ): Promise<boolean> {
    try {
      const record = new this.processedEventModel({
        eventId,
        consumerGroup,
        eventType,
        correlationId,
        processedAt: new Date(),
        expireAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
      });

      if (session) {
        await record.save({ session });
      } else {
        await record.save();
      }

      return true;
    } catch (error: any) {
      // Código 11000 en MongoDB indica violación de índice único (duplicado)
      if (error.code === 11000 || error.name === 'MongoServerError') {
        this.logger.warn(
          `[IDEMPOTENCY] Evento duplicado descartado: eventId=${eventId}, consumerGroup=${consumerGroup}, eventType=${eventType}`,
        );
        return false;
      }
      throw error;
    }
  }

  /**
   * Ejecuta una acción de negocio asegurando idempotencia.
   * Si el evento ya fue procesado, no ejecuta la acción y devuelve { executed: false }.
   */
  async executeIdempotent<T>(
    eventId: string,
    consumerGroup: string,
    eventType: string,
    correlationId: string,
    action: () => Promise<T>,
    session?: ClientSession,
  ): Promise<{ executed: boolean; result?: T }> {
    const isFirstTime = await this.tryProcess(
      eventId,
      consumerGroup,
      eventType,
      correlationId,
      session,
    );

    if (!isFirstTime) {
      return { executed: false };
    }

    const result = await action();
    return { executed: true, result };
  }
}
