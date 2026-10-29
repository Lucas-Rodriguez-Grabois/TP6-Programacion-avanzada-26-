/**
 * =================================================================================================
 * ARCHIVO: apps/activation-api/src/activations/activations.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Implementación del Agregador de la Saga y orquestación coreográfica (Sección 04, 06 y 07 del PDF).
 * 2. Método `createActivation`:
 *    - Registra la activación con estado inicial `PENDING` en MongoDB.
 *    - Construye el evento `ActivationRequested` cumpliendo el contrato común (eventId, correlationId, customerId).
 *    - Persiste en la colección Outbox y publica en el tópico Kafka `activation.requested` con key=customerId.
 *    - Retorna de inmediato la respuesta para HTTP 202 Accepted (RF-02 y RF-03).
 * 3. Máquina de estados de la Saga (Sección 06):
 *    - PENDING -> IN_PROGRESS: cuando llega el primer resultado de billing o provisioning.
 *    - IN_PROGRESS -> ACTIVE: cuando AMBOS servicios confirman éxito (BillingAccountCreated y ProvisioningCompleted).
 *      Publica `ActivationCompleted` en `activation.events` (RF-05).
 *    - PENDING/IN_PROGRESS -> FAILED: cuando CUALQUIERA de los dos servicios falla.
 *      Publica `ActivationFailed` en `activation.events` para disparar compensación en billing (RF-06).
 * 4. Integración completa con WebSocket Gateway (`ActivationGateway`) para alimentar la timeline en vivo (RF-08).
 * =================================================================================================
 */

import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import {
  TOPICS,
  EVENT_TYPES,
  createEventEnvelope,
  ActivationRequestedPayload,
  ActivationCompletedPayload,
  ActivationFailedPayload,
  EventEnvelope,
} from '@poc/contracts';
import { KafkaProducerService, OutboxService } from '@poc/kafka-toolkit';
import { Activation, ActivationDocument, ActivationStatus } from './activation.schema';
import { ActivationGateway } from '../gateway/activation.gateway';

export interface CreateActivationDto {
  customerId: string;
  planId: string;
  simulateFailure?: 'none' | 'billing' | 'provisioning';
}

@Injectable()
export class ActivationsService {
  private readonly logger = new Logger(ActivationsService.name);

  constructor(
    @InjectModel(Activation.name)
    private readonly activationModel: Model<ActivationDocument>,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly outboxService: OutboxService,
    private readonly gateway: ActivationGateway,
  ) {}

  /**
   * RF-01, RF-02, RF-03:
   * Recibe la petición, guarda en MongoDB con estado PENDING, publica ActivationRequested
   * y responde inmediatamente sin esperar a otros microservicios.
   */
  async createActivation(dto: CreateActivationDto) {
    const activationId = `act-${Math.floor(1000 + Math.random() * 9000)}`;
    const failureMode = dto.simulateFailure || 'none';

    this.logger.log(
      `[SAGA INICIO] Creando activación ${activationId} para cliente ${dto.customerId} (Plan: ${dto.planId}, Simulación: ${failureMode})`,
    );

    const initialHistory = [
      {
        eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
        at: new Date().toISOString(),
        source: 'activation-api',
        details: { planId: dto.planId, simulateFailure: failureMode },
      },
    ];

    const activation = new this.activationModel({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: 'PENDING' as ActivationStatus,
      simulateFailure: failureMode,
      steps: { billing: null, provisioning: null },
      history: initialHistory,
      createdAt: new Date(),
      updatedAt: new Date(),
    });

    await activation.save();

    // Construcción del evento formal con sobre estándar
    const event = createEventEnvelope<ActivationRequestedPayload>({
      eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
      correlationId: activationId,
      customerId: dto.customerId,
      source: 'activation-api',
      payload: {
        planId: dto.planId,
        channel: 'web',
        simulateFailure: failureMode,
      },
    });

    // Guardado en outbox y publicación a Kafka
    await this.outboxService.saveEvent(
      TOPICS.ACTIVATION_REQUESTED,
      dto.customerId,
      event,
    );
    await this.kafkaProducer.publishDirect(
      TOPICS.ACTIVATION_REQUESTED,
      dto.customerId,
      event,
    );

    // Notificación en tiempo real a la UI
    this.gateway.broadcastActivationUpdate(activation.toObject());
    this.gateway.broadcastTimelineEvent({
      activationId,
      event,
    });

    return {
      activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: 'PENDING',
      message: 'Solicitud de activación aceptada y encolada para procesamiento por eventos',
      createdAt: activation.createdAt,
    };
  }

  /**
   * RF-09: Devuelve el estado actual y el historial completo de eventos
   */
  async getActivationById(id: string): Promise<Activation> {
    const activation = await this.activationModel.findById(id).exec();
    if (!activation) {
      throw new NotFoundException(`Activación con ID ${id} no encontrada`);
    }
    return activation;
  }

  /**
   * Lista las activaciones recientes para la Demo UI
   */
  async listActivations(): Promise<Activation[]> {
    return this.activationModel
      .find()
      .sort({ createdAt: -1 })
      .limit(30)
      .exec();
  }

  /**
   * Procesa el resultado proveniente de billing-service
   */
  async processBillingResult(envelope: EventEnvelope<any>) {
    const activationId = envelope.correlationId;
    const activation = await this.activationModel.findById(activationId);
    if (!activation) {
      this.logger.warn(`Activación ${activationId} no encontrada para evento de billing`);
      return;
    }

    const isSuccess = envelope.eventType === EVENT_TYPES.BILLING_ACCOUNT_CREATED;
    const isCancelled = envelope.eventType === EVENT_TYPES.BILLING_ACCOUNT_CANCELLED;

    // Registrar en historial
    activation.history.push({
      eventType: envelope.eventType,
      at: envelope.occurredAt,
      source: envelope.source,
      details: envelope.payload,
    });

    if (isCancelled) {
      // Evento de compensación
      if (activation.steps.billing) {
        activation.steps.billing.status = 'FAILED';
        activation.steps.billing.details = { ...activation.steps.billing.details, cancelled: true };
      }
      activation.updatedAt = new Date();
      await activation.save();
      this.gateway.broadcastActivationUpdate(activation.toObject());
      this.gateway.broadcastTimelineEvent({ activationId, event: envelope });
      return;
    }

    activation.steps.billing = {
      status: isSuccess ? 'OK' : 'FAILED',
      at: envelope.occurredAt,
      details: envelope.payload,
      reason: envelope.payload?.reason,
    };

    activation.updatedAt = new Date();

    await this.evaluateSagaState(activation, envelope);
  }

  /**
   * Procesa el resultado proveniente de provisioning-service
   */
  async processProvisioningResult(envelope: EventEnvelope<any>) {
    const activationId = envelope.correlationId;
    const activation = await this.activationModel.findById(activationId);
    if (!activation) {
      this.logger.warn(`Activación ${activationId} no encontrada para evento de provisioning`);
      return;
    }

    const isSuccess = envelope.eventType === EVENT_TYPES.PROVISIONING_COMPLETED;

    // Registrar en historial
    activation.history.push({
      eventType: envelope.eventType,
      at: envelope.occurredAt,
      source: envelope.source,
      details: envelope.payload,
    });

    activation.steps.provisioning = {
      status: isSuccess ? 'OK' : 'FAILED',
      at: envelope.occurredAt,
      details: envelope.payload,
      reason: envelope.payload?.reason,
    };

    activation.updatedAt = new Date();

    await this.evaluateSagaState(activation, envelope);
  }

  /**
   * Regla de agregación de la saga (Sección 06 y 07):
   * Con los dos OK publica ActivationCompleted; con el primer fallo publica ActivationFailed.
   */
  private async evaluateSagaState(
    activation: ActivationDocument,
    incomingEnvelope: EventEnvelope<any>,
  ) {
    const activationId = activation._id;
    const billingStep = activation.steps.billing;
    const provStep = activation.steps.provisioning;

    // Si ya estaba en estado final (ACTIVE o FAILED), no cambiar estado
    if (activation.status === 'ACTIVE' || activation.status === 'FAILED') {
      await activation.save();
      this.gateway.broadcastActivationUpdate(activation.toObject());
      this.gateway.broadcastTimelineEvent({ activationId, event: incomingEnvelope });
      return;
    }

    // Caso 1: Alguno de los dos falló -> Pasa a FAILED y publica ActivationFailed
    if (billingStep?.status === 'FAILED' || provStep?.status === 'FAILED') {
      activation.status = 'FAILED';
      const failedStep = billingStep?.status === 'FAILED' ? 'billing' : 'provisioning';
      const reason =
        (failedStep === 'billing' ? billingStep?.reason : provStep?.reason) ||
        'Error forzado o técnico durante la activación';

      this.logger.warn(`[SAGA FAILED] Activación ${activationId} marcada como FAILED. Motivo: ${reason}`);

      const failedEnvelope = createEventEnvelope<ActivationFailedPayload>({
        eventType: EVENT_TYPES.ACTIVATION_FAILED,
        correlationId: activationId,
        customerId: activation.customerId,
        source: 'activation-api',
        payload: {
          reason,
          failedStep,
          failedAt: new Date().toISOString(),
        },
      });

      activation.history.push({
        eventType: EVENT_TYPES.ACTIVATION_FAILED,
        at: failedEnvelope.occurredAt,
        source: 'activation-api',
        details: failedEnvelope.payload,
      });

      await activation.save();

      // Guardar en outbox y publicar a Kafka para compensación y notificación
      await this.outboxService.saveEvent(TOPICS.ACTIVATION_EVENTS, activation.customerId, failedEnvelope);
      await this.kafkaProducer.publishDirect(TOPICS.ACTIVATION_EVENTS, activation.customerId, failedEnvelope);

      this.gateway.broadcastActivationUpdate(activation.toObject());
      this.gateway.broadcastTimelineEvent({ activationId, event: incomingEnvelope });
      this.gateway.broadcastTimelineEvent({ activationId, event: failedEnvelope });
      return;
    }

    // Caso 2: Ambos confirmaron OK -> Pasa a ACTIVE y publica ActivationCompleted
    if (billingStep?.status === 'OK' && provStep?.status === 'OK') {
      activation.status = 'ACTIVE';
      this.logger.log(`[SAGA COMPLETADA CON ÉXITO] Activación ${activationId} completada. Estado: ACTIVE`);

      const completedEnvelope = createEventEnvelope<ActivationCompletedPayload>({
        eventType: EVENT_TYPES.ACTIVATION_COMPLETED,
        correlationId: activationId,
        customerId: activation.customerId,
        source: 'activation-api',
        payload: {
          planId: activation.planId,
          completedAt: new Date().toISOString(),
          billingAccountId: billingStep.details?.billingAccountId,
          provisioningOrderId: provStep.details?.orderId,
        },
      });

      activation.history.push({
        eventType: EVENT_TYPES.ACTIVATION_COMPLETED,
        at: completedEnvelope.occurredAt,
        source: 'activation-api',
        details: completedEnvelope.payload,
      });

      await activation.save();

      // Publicar en Kafka
      await this.outboxService.saveEvent(TOPICS.ACTIVATION_EVENTS, activation.customerId, completedEnvelope);
      await this.kafkaProducer.publishDirect(TOPICS.ACTIVATION_EVENTS, activation.customerId, completedEnvelope);

      this.gateway.broadcastActivationUpdate(activation.toObject());
      this.gateway.broadcastTimelineEvent({ activationId, event: incomingEnvelope });
      this.gateway.broadcastTimelineEvent({ activationId, event: completedEnvelope });
      return;
    }

    // Caso 3: Solo uno confirmó -> Pasa a IN_PROGRESS
    activation.status = 'IN_PROGRESS';
    await activation.save();
    this.gateway.broadcastActivationUpdate(activation.toObject());
    this.gateway.broadcastTimelineEvent({ activationId, event: incomingEnvelope });
  }
}
