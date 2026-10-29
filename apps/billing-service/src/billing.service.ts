/**
 * =================================================================================================
 * ARCHIVO: apps/billing-service/src/billing.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Lógica de negocio del microservicio de facturación (RF-04 y RF-06).
 * 2. Procesamiento de `ActivationRequested`:
 *    - Evaluación de `simulateFailure`: Si es 'billing', emite `BillingFailed` (error de negocio simulado, sin ir a DLQ).
 *    - Si es exitoso, crea el registro en `billing_accounts` y emite `BillingAccountCreated` a `billing.events`.
 * 3. Patrón de Compensación de la Saga (RF-06):
 *    - Escucha `ActivationFailed` de `activation.events`.
 *    - Anula la cuenta creada pasando su estado a `CANCELLED` y emitiendo `BillingAccountCancelled`.
 * 4. Manejo del Caso Borde (Sección 06 y 08 del PDF):
 *    - Si provisioning falla de inmediato antes de que billing termine de crear la cuenta,
 *      se registra la intención de anulación para que no quede ninguna cuenta activa en caso de carreras.
 * 5. Integración con Outbox y clave de partición por `customerId` (RNF-02).
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
  ActivationFailedPayload,
  BillingAccountCreatedPayload,
  BillingFailedPayload,
  BillingAccountCancelledPayload,
} from '@poc/contracts';
import { KafkaProducerService, OutboxService } from '@poc/kafka-toolkit';
import { BillingAccount, BillingAccountDocument } from './schemas/billing-account.schema';

@Injectable()
export class BillingService {
  private readonly logger = new Logger(BillingService.name);

  // Registro en memoria de fallos prematuros para mitigar condiciones de carrera de compensación
  private readonly earlyCancellations = new Set<string>();

  constructor(
    @InjectModel(BillingAccount.name)
    private readonly billingModel: Model<BillingAccountDocument>,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly outboxService: OutboxService,
  ) {}

  /**
   * Procesa la solicitud de activación (RF-04)
   */
  async processActivationRequested(envelope: EventEnvelope<ActivationRequestedPayload>) {
    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const { simulateFailure } = envelope.payload;

    this.logger.log(
      `[BILLING] Procesando ActivationRequested para activación ${activationId}, cliente ${customerId}`,
    );

    // Caso de simulación de fallo a pedido
    if (simulateFailure === 'billing') {
      this.logger.warn(`[BILLING] Simulando fallo forzado en facturación para ${activationId}`);

      const failureEvent = createEventEnvelope<BillingFailedPayload>({
        eventType: EVENT_TYPES.BILLING_FAILED,
        correlationId: activationId,
        customerId,
        source: 'billing-service',
        payload: {
          reason: 'Fallo simulado en pasarela de cobro bancario (simulateFailure=billing)',
          code: 'SIMULATED_BILLING_FAILURE',
        },
      });

      await this.outboxService.saveEvent(TOPICS.BILLING_EVENTS, customerId, failureEvent);
      await this.kafkaProducer.publishDirect(TOPICS.BILLING_EVENTS, customerId, failureEvent);
      return;
    }

    // Verificar si ya llegó un ActivationFailed antes de que billing cree la cuenta (Caso Borde)
    const wasCancelledEarly = this.earlyCancellations.has(activationId);
    const initialStatus = wasCancelledEarly ? 'CANCELLED' : 'CREATED';

    const billingAccount = new this.billingModel({
      activationId,
      customerId,
      status: initialStatus,
      monthlyFee: 4500,
      cancellationReason: wasCancelledEarly ? 'Cancelado preventivamente por fallo en provisioning' : undefined,
    });

    try {
      await billingAccount.save();
    } catch (err: any) {
      if (err.code === 11000) {
        this.logger.warn(`[BILLING] Cuenta de facturación para ${activationId} ya existe`);
        return;
      }
      throw err;
    }

    if (wasCancelledEarly) {
      this.logger.warn(
        `[BILLING] Caso borde resuelto: La cuenta para ${activationId} se creó directamente como CANCELLED`,
      );
      return;
    }

    // Emisión de éxito: BillingAccountCreated
    const successEvent = createEventEnvelope<BillingAccountCreatedPayload>({
      eventType: EVENT_TYPES.BILLING_ACCOUNT_CREATED,
      correlationId: activationId,
      customerId,
      source: 'billing-service',
      payload: {
        billingAccountId: `bill-${activationId.replace('act-', '')}`,
        monthlyFee: 4500,
        status: 'CREATED',
      },
    });

    await this.outboxService.saveEvent(TOPICS.BILLING_EVENTS, customerId, successEvent);
    await this.kafkaProducer.publishDirect(TOPICS.BILLING_EVENTS, customerId, successEvent);
  }

  /**
   * RF-06: Compensación de la Saga cuando ocurre ActivationFailed
   */
  async processActivationFailed(envelope: EventEnvelope<ActivationFailedPayload>) {
    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const reason = envelope.payload?.reason || 'Compensación por activación fallida';

    this.logger.warn(
      `[BILLING COMPENSACIÓN] Recibido ActivationFailed para ${activationId}. Procediendo a anular cuenta.`,
    );

    // Guardar en cache de cancelaciones tempranas para caso borde
    this.earlyCancellations.add(activationId);

    const account = await this.billingModel.findOne({ activationId });
    if (!account) {
      this.logger.warn(
        `[BILLING] No se encontró cuenta aún para ${activationId}; se marcó como cancelada anticipadamente`,
      );
      return;
    }

    if (account.status === 'CANCELLED') {
      this.logger.log(`[BILLING] La cuenta ${activationId} ya se encontraba cancelada`);
      return;
    }

    account.status = 'CANCELLED';
    account.cancellationReason = reason;
    await account.save();

    // Emisión del evento de compensación: BillingAccountCancelled
    const cancelEvent = createEventEnvelope<BillingAccountCancelledPayload>({
      eventType: EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
      correlationId: activationId,
      customerId,
      source: 'billing-service',
      payload: {
        billingAccountId: `bill-${activationId.replace('act-', '')}`,
        reason,
        cancelledAt: new Date().toISOString(),
      },
    });

    await this.outboxService.saveEvent(TOPICS.BILLING_EVENTS, customerId, cancelEvent);
    await this.kafkaProducer.publishDirect(TOPICS.BILLING_EVENTS, customerId, cancelEvent);

    this.logger.log(`[BILLING COMPENSADO] Cuenta de facturación para ${activationId} anulada con éxito`);
  }
}
