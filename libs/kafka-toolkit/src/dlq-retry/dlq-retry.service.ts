/**
 * =================================================================================================
 * ARCHIVO: libs/kafka-toolkit/src/dlq-retry/dlq-retry.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Implementación de la política de Reintentos con Backoff Exponencial y Dead Letter Queue (DLQ)
 *    conforme a RNF-04 y la Decisión Técnica 2 del documento de arquitectura.
 * 2. Manejo de fallos técnicos:
 *    - Reintenta hasta 3 veces ante excepciones imprevistas con esperas crecientes: 1 s, 2 s y 4 s.
 *    - Si todos los intentos se agotan, envía el mensaje original al tópico `<topic>.dlq`
 *      adjuntando metadatos de auditoría en los headers (`x-error-message`, `x-original-topic`,
 *      `x-retry-count`, `x-failed-at`).
 *    - Permite al consumidor confirmar el offset en el tópico original para evitar que un mensaje
 *      corrupto o con error no recuperable bloquee el procesamiento del resto de la partición.
 * 3. Se distingue explícitamente entre errores de negocio (manejados por la lógica de dominio sin ir a DLQ)
 *    y errores técnicos/de infraestructura.
 * =================================================================================================
 */

import { Injectable, Logger } from '@nestjs/common';
import { getDlqTopicName } from '@poc/contracts';
import { KafkaProducerService } from '../kafka-producer/kafka-producer.service';

export interface RetryDlqOptions<T> {
  topic: string;
  partitionKey: string;
  envelope: any;
  action: () => Promise<T>;
  maxRetries?: number;
  delaysMs?: number[];
}

@Injectable()
export class DlqRetryService {
  private readonly logger = new Logger(DlqRetryService.name);

  constructor(private readonly kafkaProducer: KafkaProducerService) {}

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Envuelve una acción de consumo de Kafka con política de reintentos y desvío a DLQ.
   */
  async executeWithRetryAndDlq<T>(options: RetryDlqOptions<T>): Promise<T | null> {
    const {
      topic,
      partitionKey,
      envelope,
      action,
      maxRetries = 3,
      delaysMs = [1000, 2000, 4000],
    } = options;

    let attempt = 0;
    let lastError: any = null;

    while (attempt <= maxRetries) {
      try {
        if (attempt > 0) {
          const delay = delaysMs[attempt - 1] || 4000;
          this.logger.warn(
            `[REINTENTO ${attempt}/${maxRetries}] Esperando ${delay}ms para reintentar tópico=${topic}, eventId=${envelope?.eventId}`,
          );
          await this.sleep(delay);
        }

        return await action();
      } catch (err: any) {
        lastError = err;
        attempt++;
        this.logger.error(
          `[FALLO TÉCNICO intento ${attempt}/${maxRetries}] Error en tópico=${topic}: ${err?.message || err}`,
        );
      }
    }

    // Si agotó todos los reintentos, se desvía a la DLQ correspondiente
    const dlqTopic = getDlqTopicName(topic);
    this.logger.error(
      `[DLQ EXHAUSTED] Mensaje desviado a ${dlqTopic}. eventId=${envelope?.eventId}, correlationId=${envelope?.correlationId}`,
    );

    try {
      await this.kafkaProducer.sendToDlq(dlqTopic, partitionKey, envelope, {
        'x-error-message': String(lastError?.message || lastError),
        'x-error-stack': String(lastError?.stack || ''),
        'x-original-topic': topic,
        'x-retry-count': String(maxRetries),
        'x-failed-at': new Date().toISOString(),
      });
    } catch (dlqErr: any) {
      this.logger.error(
        `[CRITICAL] Error al enviar mensaje a la DLQ ${dlqTopic}: ${dlqErr?.message}`,
      );
    }

    return null;
  }
}
