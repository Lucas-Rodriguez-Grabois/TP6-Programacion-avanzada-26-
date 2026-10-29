/**
 * =================================================================================================
 * ARCHIVO: apps/activation-api/src/activations/activations.controller.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Controlador HTTP REST y Consumidor de Eventos Kafka para `activation-api`.
 * 2. Endpoints HTTP implementados:
 *    - `POST /activations`:
 *      * Recibe { customerId, planId, simulateFailure }.
 *      * Responde HTTP 202 ACCEPTED de manera asíncrona sin bloquear la respuesta esperando a los
 *        sistemas downstream (RF-02).
 *    - `GET /activations/:id`:
 *      * Consulta el estado actual y el historial completo de la saga de activación (RF-09).
 *    - `GET /activations`:
 *      * Retorna el listado de las activaciones recientes para alimentar la Demo UI.
 * 3. Patrones de eventos Kafka (`@EventPattern`):
 *    - Escucha `billing.events` y delega a `processBillingResult`.
 *    - Escucha `provisioning.events` y delega a `processProvisioningResult`.
 *    - Integración con IdempotencyService (RNF-03) y DlqRetryService (RNF-04).
 * =================================================================================================
 */

import { Controller, Post, Get, Body, Param, HttpCode, HttpStatus, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, CONSUMER_GROUPS, EventEnvelope } from '@poc/contracts';
import { IdempotencyService, DlqRetryService } from '@poc/kafka-toolkit';
import { ActivationsService, CreateActivationDto } from './activations.service';

@Controller('activations')
export class ActivationsController {
  private readonly logger = new Logger(ActivationsController.name);

  constructor(
    private readonly activationsService: ActivationsService,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqRetryService: DlqRetryService,
  ) {}

  /**
   * RF-01 y RF-02: Creación de la activación con código 202 Accepted
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  async createActivation(@Body() dto: CreateActivationDto) {
    return this.activationsService.createActivation(dto);
  }

  /**
   * RF-09: Consulta de estado e historial por activationId
   */
  @Get(':id')
  async getActivation(@Param('id') id: string) {
    return this.activationsService.getActivationById(id);
  }

  /**
   * Listado de activaciones para la UI de demostración
   */
  @Get()
  async listActivations() {
    return this.activationsService.listActivations();
  }

  /**
   * Consumidor de eventos de facturación (billing.events)
   */
  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(@Payload() message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      this.logger.warn(`Mensaje sin sobre válido en ${TOPICS.BILLING_EVENTS}`);
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic: TOPICS.BILLING_EVENTS,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.ACTIVATION_API,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.activationsService.processBillingResult(envelope);
          },
        );
      },
    });
  }

  /**
   * Consumidor de eventos de aprovisionamiento (provisioning.events)
   */
  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() message: any) {
    const envelope: EventEnvelope<any> = typeof message === 'string' ? JSON.parse(message) : message;

    if (!envelope || !envelope.eventId) {
      this.logger.warn(`Mensaje sin sobre válido en ${TOPICS.PROVISIONING_EVENTS}`);
      return;
    }

    await this.dlqRetryService.executeWithRetryAndDlq({
      topic: TOPICS.PROVISIONING_EVENTS,
      partitionKey: envelope.customerId,
      envelope,
      action: async () => {
        await this.idempotencyService.executeIdempotent(
          envelope.eventId,
          CONSUMER_GROUPS.ACTIVATION_API,
          envelope.eventType,
          envelope.correlationId,
          async () => {
            await this.activationsService.processProvisioningResult(envelope);
          },
        );
      },
    });
  }
}
