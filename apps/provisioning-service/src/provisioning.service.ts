/**
 * =================================================================================================
 * ARCHIVO: apps/provisioning-service/src/provisioning.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Lógica de negocio del microservicio de aprovisionamiento de red (RF-04).
 * 2. Simulación de retardo asíncrono de 1 a 3 segundos para emular la interacción física con la
 *    infraestructura de red/equipamiento del operador de telecomunicaciones.
 * 3. Soporte para fallos forzados a pedido (simulateFailure === 'provisioning'):
 *    - Publica el evento `ProvisioningFailed` a `provisioning.events` con clave de cliente `customerId`.
 *    - No va a DLQ al ser un escenario de negocio simulado para la prueba de compensación.
 * 4. Camino feliz:
 *    - Genera el identificador de orden (`orderId`) y circuito (`circuitId`).
 *    - Persiste en `provisioning_orders` y emite `ProvisioningCompleted` a `provisioning.events`.
 * 5. Registro confiable en la colección Outbox y clave de partición garantizada (RNF-02).
 * =================================================================================================
 */

import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import {
  TOPICS,
  EVENT_TYPES,
  createEventEnvelope,
  EventEnvelope,
  ActivationRequestedPayload,
  ProvisioningCompletedPayload,
  ProvisioningFailedPayload,
} from '@poc/contracts';
import { KafkaProducerService, OutboxService } from '@poc/kafka-toolkit';
import {
  ProvisioningOrder,
  ProvisioningOrderDocument,
} from './schemas/provisioning-order.schema';

@Injectable()
export class ProvisioningService {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectModel(ProvisioningOrder.name)
    private readonly orderModel: Model<ProvisioningOrderDocument>,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly outboxService: OutboxService,
  ) {}

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Procesa la solicitud de aprovisionamiento simulando retardo de red de 1 a 3 segundos (RF-04).
   */
  async processActivationRequested(envelope: EventEnvelope<ActivationRequestedPayload>) {
    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const { simulateFailure } = envelope.payload;

    // Retardo simulado entre 1000 y 2500 ms (1 a 3 segundos)
    const simulatedDelay = Math.floor(Math.random() * 1500) + 1000;
    this.logger.log(
      `[PROVISIONING] Iniciando aprovisionamiento para ${activationId} (espera de ${simulatedDelay}ms)...`,
    );
    await this.sleep(simulatedDelay);

    // Caso de simulación forzada de fallo en aprovisionamiento
    if (simulateFailure === 'provisioning') {
      this.logger.warn(`[PROVISIONING] Simulando fallo forzado de red para ${activationId}`);

      const failureEvent = createEventEnvelope<ProvisioningFailedPayload>({
        eventType: EVENT_TYPES.PROVISIONING_FAILED,
        correlationId: activationId,
        customerId,
        source: 'provisioning-service',
        payload: {
          reason: 'Fallo simulado en la asignación de puerto DSLAM / OLT (simulateFailure=provisioning)',
          code: 'PROV_EQUIPMENT_UNAVAILABLE',
        },
      });

      await this.outboxService.saveEvent(TOPICS.PROVISIONING_EVENTS, customerId, failureEvent);
      await this.kafkaProducer.publishDirect(TOPICS.PROVISIONING_EVENTS, customerId, failureEvent);
      return;
    }

    // Camino feliz
    const orderId = `prov-ord-${activationId.replace('act-', '')}`;
    const circuitId = `CIRC-${Math.floor(100000 + Math.random() * 900000)}`;

    const order = new this.orderModel({
      activationId,
      customerId,
      orderId,
      circuitId,
      status: 'COMPLETED',
      completedAt: new Date(),
    });

    try {
      await order.save();
    } catch (err: any) {
      if (err.code === 11000) {
        this.logger.warn(`[PROVISIONING] Orden para ${activationId} ya fue registrada`);
        return;
      }
      throw err;
    }

    const successEvent = createEventEnvelope<ProvisioningCompletedPayload>({
      eventType: EVENT_TYPES.PROVISIONING_COMPLETED,
      correlationId: activationId,
      customerId,
      source: 'provisioning-service',
      payload: {
        orderId,
        circuitId,
        completedAt: new Date().toISOString(),
      },
    });

    await this.outboxService.saveEvent(TOPICS.PROVISIONING_EVENTS, customerId, successEvent);
    await this.kafkaProducer.publishDirect(TOPICS.PROVISIONING_EVENTS, customerId, successEvent);

    this.logger.log(`[PROVISIONING OK] Línea aprovisionada con éxito para ${activationId}`);
  }
}
