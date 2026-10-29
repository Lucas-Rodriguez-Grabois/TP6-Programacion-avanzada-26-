/**
 * =================================================================================================
 * ARCHIVO: apps/notification-service/src/notification.controller.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Controlador Kafka para el microservicio `notification-service`.
 * 2. Suscripción al tópico `TOPICS.ACTIVATION_EVENTS` (`activation.events`) con el grupo `notification-svc`.
 * 3. Procesa los eventos terminales de la activación:
 *    - `ActivationCompleted`: Despacha email de bienvenida en Mailhog (RF-07).
 *    - `ActivationFailed`: Despacha email de notificación de fallo en Mailhog (RF-07).
 * 4. Idempotencia estricta (RNF-03): Reenviar un evento procesado no genera emails duplicados.
 * 5. Reintentos y DLQ (RNF-04) para protección ante caídas temporales del servidor SMTP.
 * =================================================================================================
 */

import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, EVENT_TYPES, CONSUMER_GROUPS, EventEnvelope } from '@poc/contracts';
import { IdempotencyService, DlqRetryService } from '@poc/kafka-toolkit';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  private readonly logger = new Logger(NotificationController.name);

  constructor(
    private readonly notificationService: NotificationService,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqRetryService: DlqRetryService,
  ) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      return;
    }

    // Solo procesar ActivationCompleted y ActivationFailed
    if (
      envelope.eventType !== EVENT_TYPES.ACTIVATION_COMPLETED &&
      envelope.eventType !== EVENT_TYPES.ACTIVATION_FAILED
    ) {
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic: TOPICS.ACTIVATION_EVENTS,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.NOTIFICATION_SERVICE,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            if (envelope.eventType === EVENT_TYPES.ACTIVATION_COMPLETED) {
              await this.notificationService.sendWelcomeEmail(envelope);
            } else if (envelope.eventType === EVENT_TYPES.ACTIVATION_FAILED) {
              await this.notificationService.sendFailureEmail(envelope);
            }
          },
        );
      },
    });
  }
}
