/**
 * =================================================================================================
 * ARCHIVO: apps/loyalty-service/src/loyalty.controller.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Controlador Kafka para `loyalty-service`.
 * 2. Suscripción al tópico `TOPICS.ACTIVATION_EVENTS` (`activation.events`).
 * 3. Reacciona a `ActivationCompleted` delegando al servicio de fidelización.
 * 4. Aplica idempotencia sobre la colección `processed_events` del grupo `loyalty-group`.
 * =================================================================================================
 */

import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES, CONSUMER_GROUPS, EventEnvelope } from '@poc/contracts';
import { IdempotencyService, DlqRetryService } from '@poc/kafka-toolkit';
import { LoyaltyService } from './loyalty.service';

@Controller()
export class LoyaltyController {
  private readonly logger = new Logger(LoyaltyController.name);

  constructor(
    private readonly loyaltyService: LoyaltyService,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqRetryService: DlqRetryService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      return;
    }

    if (envelope.eventType !== EVENT_TYPES.ACTIVATION_COMPLETED) {
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic: TOPICS.ACTIVATION_EVENTS,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.LOYALTY_SERVICE,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.loyaltyService.awardPointsForActivation(envelope);
          },
        );
      },
    });
  }
}
