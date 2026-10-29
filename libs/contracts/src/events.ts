/**
 * =================================================================================================
 * ARCHIVO: libs/contracts/src/events.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición exhaustiva de las constantes de nombres de eventos (EVENT_TYPES) en tiempo pasado:
 *    - ActivationRequested, BillingAccountCreated, BillingFailed, BillingAccountCancelled,
 *      ProvisioningCompleted, ProvisioningFailed, ActivationCompleted, ActivationFailed,
 *      LoyaltyPointsAwarded.
 * 2. Definición de las interfaces de payload para cada tipo de evento del negocio de telecomunicaciones:
 *    - ActivationRequestedPayload: soporte para customerId, planId, channel y simulación de fallos forzados (RF-01).
 *    - BillingAccountCreatedPayload y BillingFailedPayload: respuestas del servicio de facturación (RF-04).
 *    - BillingAccountCancelledPayload: evento de compensación de la saga (RF-06).
 *    - ProvisioningCompletedPayload y ProvisioningFailedPayload: respuestas de red/aprovisionamiento (RF-04).
 *    - ActivationCompletedPayload y ActivationFailedPayload: resoluciones del agregador de saga (RF-05, RF-06).
 *    - LoyaltyPointsAwardedPayload: evento para el servicio loyalty del escenario 5.
 * 3. Tipado estricto de los sobres EventEnvelope correspondientes a cada evento.
 * =================================================================================================
 */

import { EventEnvelope } from './envelope';

export const EVENT_TYPES = {
  ACTIVATION_REQUESTED: 'ActivationRequested',
  BILLING_ACCOUNT_CREATED: 'BillingAccountCreated',
  BILLING_FAILED: 'BillingFailed',
  BILLING_ACCOUNT_CANCELLED: 'BillingAccountCancelled',
  PROVISIONING_COMPLETED: 'ProvisioningCompleted',
  PROVISIONING_FAILED: 'ProvisioningFailed',
  ACTIVATION_COMPLETED: 'ActivationCompleted',
  ACTIVATION_FAILED: 'ActivationFailed',
  LOYALTY_POINTS_AWARDED: 'LoyaltyPointsAwarded',
} as const;

export type SimulateFailureMode = 'none' | 'billing' | 'provisioning';

// -------------------------------------------------------------------------
// Payloads
// -------------------------------------------------------------------------

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailureMode;
}

export interface BillingAccountCreatedPayload {
  billingAccountId: string;
  monthlyFee: number;
  status: 'ACTIVE' | 'CREATED';
}

export interface BillingFailedPayload {
  reason: string;
  code: string;
}

export interface BillingAccountCancelledPayload {
  billingAccountId?: string;
  reason: string;
  cancelledAt: string;
}

export interface ProvisioningCompletedPayload {
  orderId: string;
  circuitId: string;
  completedAt: string;
}

export interface ProvisioningFailedPayload {
  reason: string;
  code: string;
}

export interface ActivationCompletedPayload {
  planId: string;
  completedAt: string;
  billingAccountId?: string;
  provisioningOrderId?: string;
}

export interface ActivationFailedPayload {
  reason: string;
  failedStep: 'billing' | 'provisioning' | 'system';
  failedAt: string;
}

export interface LoyaltyPointsAwardedPayload {
  customerId: string;
  points: number;
  totalPoints: number;
  awardedAt: string;
}

// -------------------------------------------------------------------------
// Typed Envelopes
// -------------------------------------------------------------------------

export type ActivationRequestedEvent = EventEnvelope<ActivationRequestedPayload>;
export type BillingAccountCreatedEvent = EventEnvelope<BillingAccountCreatedPayload>;
export type BillingFailedEvent = EventEnvelope<BillingFailedPayload>;
export type BillingAccountCancelledEvent = EventEnvelope<BillingAccountCancelledPayload>;
export type ProvisioningCompletedEvent = EventEnvelope<ProvisioningCompletedPayload>;
export type ProvisioningFailedEvent = EventEnvelope<ProvisioningFailedPayload>;
export type ActivationCompletedEvent = EventEnvelope<ActivationCompletedPayload>;
export type ActivationFailedEvent = EventEnvelope<ActivationFailedPayload>;
export type LoyaltyPointsAwardedEvent = EventEnvelope<LoyaltyPointsAwardedPayload>;
