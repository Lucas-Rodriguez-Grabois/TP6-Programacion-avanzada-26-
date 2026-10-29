/**
 * =================================================================================================
 * ARCHIVO: apps/provisioning-service/src/provisioning.controller.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Controlador Kafka para el microservicio `provisioning-service`.
 * 2. Suscripción al tópico `TOPICS.ACTIVATION_REQUESTED` (`activation.requested`).
 * 3. Consume el evento `ActivationRequested` en paralelo con `billing-service` (RF-04).
 * 4. Aplica idempotencia en base a `eventId` y `consumerGroup` (`provisioning-svc`) para evitar
 *    doble asignación de equipamiento ante reentregas (RNF-03).
 * 5. Protegido por la política de 3 reintentos y envío a DLQ (`DlqRetryService`, RNF-04).
 * =================================================================================================
 */

import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, CONSUMER_GROUPS, EventEnvelope } from '@poc/contracts';
import { IdempotencyService, DlqRetryService } from '@poc/kafka-toolkit';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  private readonly logger = new Logger(ProvisioningController.name);

  constructor(
    private readonly provisioningService: ProvisioningService,
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
          CONSUMER_GROUPS.PROVISIONING_SERVICE,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.provisioningService.processActivationRequested(envelope);
          },
        );
      },
    });
  }
}
