/**
 * =================================================================================================
 * ARCHIVO: apps/notification-service/src/notification.service.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Lógica de negocio para el envío de correos electrónicos simulados mediante Mailhog (RF-07).
 * 2. Integración con `nodemailer` mediante transporte SMTP apuntando a Mailhog (puerto 1025).
 * 3. Métodos implementados:
 *    - `sendWelcomeEmail`: Reacciona a `ActivationCompleted`. Genera un correo de bienvenida
 *      con los detalles del plan activo, fecha y referencia de correlación.
 *    - `sendFailureEmail`: Reacciona a `ActivationFailed`. Genera un correo de aviso informando
 *      el fallo y confirmando que la cuenta de cobro fue anulada de forma automática por la compensación.
 * 4. Registro de auditoría de cada email en la colección `notifications` de MongoDB.
 * 5. Soporte para el Escenario 3 del guion de demo (resiliencia ante caída de servicio y lag).
 * =================================================================================================
 */

import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as nodemailer from 'nodemailer';
import {
  EventEnvelope,
  ActivationCompletedPayload,
  ActivationFailedPayload,
} from '@poc/contracts';
import {
  NotificationRecord,
  NotificationDocument,
} from './schemas/notification.schema';

@Injectable()
export class NotificationService implements OnModuleInit {
  private readonly logger = new Logger(NotificationService.name);
  private transporter: nodemailer.Transporter;

  constructor(
    @InjectModel(NotificationRecord.name)
    private readonly notificationModel: Model<NotificationDocument>,
  ) {}

  onModuleInit() {
    const host = process.env.MAILHOG_HOST || 'localhost';
    const port = Number(process.env.MAILHOG_PORT) || 1025;

    this.transporter = nodemailer.createTransport({
      host,
      port,
      ignoreTLS: true,
    });

    this.logger.log(`[NOTIFICATION] Conectado a Mailhog SMTP en ${host}:${port}`);
  }

  /**
   * RF-07: Envío de correo de bienvenida tras activación exitosa
   */
  async sendWelcomeEmail(envelope: EventEnvelope<ActivationCompletedPayload>) {
    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const planId = envelope.payload.planId;
    const recipient = `${customerId.toLowerCase()}@demo-telecom.com`;
    const subject = `🎉 ¡Bienvenido! Tu plan ${planId} está activo (Ref: ${activationId})`;

    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;">
        <h2 style="color: #2e7d32;">¡Tu servicio ha sido activado con éxito!</h2>
        <p>Estimado cliente <strong>${customerId}</strong>,</p>
        <p>Nos complace informarte que la activación de tu plan ha finalizado satisfactoriamente.</p>
        <hr style="border: 0; border-top: 1px solid #eee;" />
        <ul>
          <li><strong>ID de Activación:</strong> ${activationId}</li>
          <li><strong>Plan contratado:</strong> ${planId}</li>
          <li><strong>Fecha de activación:</strong> ${envelope.payload.completedAt}</li>
          <li><strong>ID de Evento:</strong> ${envelope.eventId}</li>
        </ul>
        <p style="color: #666; font-size: 13px;">POC de Sistemas Distribuidos 2026 - Arquitectura orientada a eventos con Apache Kafka</p>
      </div>
    `;

    try {
      await this.transporter.sendMail({
        from: '"Telecom Service" <no-reply@demo-telecom.com>',
        to: recipient,
        subject,
        html,
      });

      this.logger.log(`[EMAIL ENVIADO] Correo de bienvenida enviado a ${recipient} para ${activationId}`);

      await new this.notificationModel({
        activationId,
        eventType: envelope.eventType,
        recipient,
        subject,
        body: html,
        status: 'SENT',
        sentAt: new Date(),
      }).save();
    } catch (err: any) {
      this.logger.error(`[NOTIFICATION ERROR] Error enviando email a Mailhog: ${err?.message}`);
      throw err;
    }
  }

  /**
   * RF-07: Envío de correo de notificación ante activación fallida
   */
  async sendFailureEmail(envelope: EventEnvelope<ActivationFailedPayload>) {
    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const reason = envelope.payload.reason;
    const failedStep = envelope.payload.failedStep;
    const recipient = `${customerId.toLowerCase()}@demo-telecom.com`;
    const subject = `⚠️ Aviso sobre tu solicitud de activación (Ref: ${activationId})`;

    const html = `
      <div style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; padding: 20px; border: 1px solid #ffcdd2; border-radius: 8px; background-color: #fff9f9;">
        <h2 style="color: #c62828;">No pudimos completar tu activación</h2>
        <p>Estimado cliente <strong>${customerId}</strong>,</p>
        <p>Hubo un inconveniente durante el proceso de activación de tu línea.</p>
        <hr style="border: 0; border-top: 1px solid #ffcdd2;" />
        <ul>
          <li><strong>ID de Activación:</strong> ${activationId}</li>
          <li><strong>Etapa que falló:</strong> ${failedStep.toUpperCase()}</li>
          <li><strong>Motivo:</strong> ${reason}</li>
          <li><strong>Compensación:</strong> Cualquier cargo o cuenta temporal ha sido anulada de forma automática.</li>
        </ul>
        <p style="color: #666; font-size: 13px;">POC de Sistemas Distribuidos 2026 - Arquitectura orientada a eventos con Apache Kafka</p>
      </div>
    `;

    try {
      await this.transporter.sendMail({
        from: '"Telecom Service" <no-reply@demo-telecom.com>',
        to: recipient,
        subject,
        html,
      });

      this.logger.log(`[EMAIL ENVIADO] Correo de error enviado a ${recipient} para ${activationId}`);

      await new this.notificationModel({
        activationId,
        eventType: envelope.eventType,
        recipient,
        subject,
        body: html,
        status: 'SENT',
        sentAt: new Date(),
      }).save();
    } catch (err: any) {
      this.logger.error(`[NOTIFICATION ERROR] Error enviando email de error a Mailhog: ${err?.message}`);
      throw err;
    }
  }
}
