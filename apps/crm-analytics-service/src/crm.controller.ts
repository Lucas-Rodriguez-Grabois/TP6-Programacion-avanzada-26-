/**
 * =================================================================================================
 * ARCHIVO: apps/crm-analytics-service/src/crm.controller.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Controlador Kafka para `crm-analytics-service`.
 * 2. Suscripción integral a TODOS los tópicos de negocio del sistema:
 *    - `TOPICS.ACTIVATION_REQUESTED`
 *    - `TOPICS.BILLING_EVENTS`
 *    - `TOPICS.PROVISIONING_EVENTS`
 *    - `TOPICS.ACTIVATION_EVENTS`
 * 3. No publica ningún evento posterior: es un consumidor puro de observabilidad y análisis de negocio (RF-10).
 * 4. Integrado con `IdempotencyService` y `DlqRetryService`.
 * =================================================================================================
 */

import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, CONSUMER_GROUPS, EventEnvelope } from '@poc/contracts';
import { IdempotencyService, DlqRetryService } from '@poc/kafka-toolkit';
import { CrmService } from './crm.service';

@Controller()
export class CrmController {
  private readonly logger = new Logger(CrmController.name);

  constructor(
    private readonly crmService: CrmService,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqRetryService: DlqRetryService,
  ) {}

  private async handleTopicEvent(topic: string, message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.CRM_ANALYTICS,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.crmService.logEvent(envelope, topic);
          },
        );
      },
    });
  }

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() message: any) {
    await this.handleTopicEvent(TOPICS.ACTIVATION_REQUESTED, message);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(@Payload() message: any) {
    await this.handleTopicEvent(TOPICS.BILLING_EVENTS, message);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() message: any) {
    await this.handleTopicEvent(TOPICS.PROVISIONING_EVENTS, message);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    await this.handleTopicEvent(TOPICS.ACTIVATION_EVENTS, message);
  }
}
