/**
 * =================================================================================================
 * ARCHIVO: apps/billing-service/src/billing.controller.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Controlador de eventos de Kafka para el microservicio `billing-service`.
 * 2. Suscripciones mediante decoradores `@EventPattern`:
 *    - `TOPICS.ACTIVATION_REQUESTED`: Reacciona a `ActivationRequested` para crear la cuenta simulada
 *      o disparar fallo forzado (RF-04).
 *    - `TOPICS.ACTIVATION_EVENTS`: Reacciona a `ActivationFailed` para ejecutar la compensación
 *      anulando la cuenta si había sido creada previamente (RF-06).
 * 3. Envoltura estricta con `IdempotencyService` (RNF-03) en base a `eventId` y `consumerGroup`,
 *    y política de reintentos más desvío a DLQ con `DlqRetryService` (RNF-04).
 * =================================================================================================
 */

import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES, CONSUMER_GROUPS, EventEnvelope } from '@poc/contracts';
import { IdempotencyService, DlqRetryService } from '@poc/kafka-toolkit';
import { BillingService } from './billing.service';

@Controller()
export class BillingController {
  private readonly logger = new Logger(BillingController.name);

  constructor(
    private readonly billingService: BillingService,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqRetryService: DlqRetryService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic: TOPICS.ACTIVATION_REQUESTED,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.BILLING_SERVICE,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.billingService.processActivationRequested(envelope);
          },
        );
      },
    });
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      return;
    }

    // Solo reaccionar ante ActivationFailed para compensación
    if (envelope.eventType !== EVENT_TYPES.ACTIVATION_FAILED) {
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic: TOPICS.ACTIVATION_EVENTS,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.BILLING_SERVICE,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.billingService.processActivationFailed(envelope);
          },
        );
      },
    });
  }
}
