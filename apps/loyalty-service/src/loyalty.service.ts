/**
 * =================================================================================================
 * ARCHIVO: apps/loyalty-service/src/loyalty.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Servicio de cálculo de beneficios de fidelidad para el Escenario 5 del Guión de Demo.
 * 2. Asigna 100 puntos de lealtad por cada activación exitosa (`ActivationCompleted`).
 * 3. Al iniciarse con `auto.offset.reset: earliest` y un Consumer Group nuevo (`loyalty-group`),
 *    lee todo el log de Kafka desde el offset cero, reconstruyendo el estado del balance de puntos
 *    de cada cliente sin alterar en absoluto a los demás servicios del cluster.
 * =================================================================================================
 */

import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventEnvelope, ActivationCompletedPayload } from '@poc/contracts';
import { LoyaltyPoint, LoyaltyPointDocument } from './schemas/loyalty-point.schema';

@Injectable()
export class LoyaltyService {
  private readonly logger = new Logger(LoyaltyService.name);

  constructor(
    @InjectModel(LoyaltyPoint.name)
    private readonly pointModel: Model<LoyaltyPointDocument>,
  ) {}

  /**
   * Acumula puntos de lealtad procesando el log de Kafka
   */
  async awardPointsForActivation(envelope: EventEnvelope<ActivationCompletedPayload>) {
    const customerId = envelope.customerId;
    const pointsToAdd = 100;

    const record = await this.pointModel.findOneAndUpdate(
      { customerId },
      {
        $inc: { points: pointsToAdd, eventsProcessed: 1 },
        $set: { lastAwardedAt: new Date() },
      },
      { upsert: true, new: true },
    );

    this.logger.log(
      `⭐ [LOYALTY REPLAY] +${pointsToAdd} puntos otorgados al cliente ${customerId} por activación ${envelope.correlationId}. Total acumulado: ${record.points} pts`,
    );
  }
}
