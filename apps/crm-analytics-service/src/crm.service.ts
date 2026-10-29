/**
 * =================================================================================================
 * ARCHIVO: apps/crm-analytics-service/src/crm.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Servicio de almacenamiento de eventos para el microservicio `crm-analytics-service` (RF-10).
 * 2. Registra cada evento recibido de cualquiera de los tópicos del cluster en la colección `event_log`.
 * 3. Demuestra la capacidad de Kafka de desacoplar emisores y receptores: Este servicio lee
 *    todas las conversaciones del cluster sin que los servicios productores conozcan de su existencia.
 * 4. Permite reprocesar el historial completo desde el inicio configurando el offset reset.
 * =================================================================================================
 */

import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventEnvelope } from '@poc/contracts';
import { EventLog, EventLogDocument } from './schemas/event-log.schema';

@Injectable()
export class CrmService {
  private readonly logger = new Logger(CrmService.name);

  constructor(
    @InjectModel(EventLog.name)
    private readonly eventLogModel: Model<EventLogDocument>,
  ) {}

  /**
   * RF-10: Registra cualquier evento recibido en el log inmutable
   */
  async logEvent(envelope: EventEnvelope<any>, topic: string) {
    try {
      const logRecord = new this.eventLogModel({
        eventId: envelope.eventId,
        correlationId: envelope.correlationId,
        customerId: envelope.customerId,
        eventType: envelope.eventType,
        source: envelope.source,
        payload: envelope.payload,
        occurredAt: envelope.occurredAt,
        receivedAt: new Date(),
      });

      await logRecord.save();

      this.logger.log(
        `[CRM ANALYTICS LOG] Tópico: ${topic} | Tipo: ${envelope.eventType} | Activación: ${envelope.correlationId} | Cliente: ${envelope.customerId}`,
      );
    } catch (err: any) {
      if (err.code === 11000) {
        this.logger.warn(
          `[CRM ANALYTICS] Evento duplicado ignorado: eventId=${envelope.eventId}`,
        );
        return;
      }
      this.logger.error(`[CRM ANALYTICS ERROR] Fallo al persistir evento: ${err?.message}`);
      throw err;
    }
  }

  /**
   * Consulta el historial acumulado
   */
  async getEventsByCorrelationId(correlationId: string): Promise<EventLog[]> {
    return this.eventLogModel.find({ correlationId }).sort({ receivedAt: 1 }).exec();
  }
}
