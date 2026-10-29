/**
 * =================================================================================================
 * ARCHIVO: libs/contracts/src/envelope.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Definición del sobre común estándar (Event Envelope) para todos los eventos del ecosistema Kafka.
 * 2. Se implementaron los campos requeridos por la especificación de la POC (Sección 05 y 06):
 *    - eventId: UUID v4 único para garantizar la idempotencia en cada consumidor.
 *    - eventType: Nombre del evento en tiempo pasado (ej. ActivationRequested, BillingAccountCreated).
 *    - version: Versión numérica del contrato (soporte para evolución de contratos sin romper consumidores).
 *    - occurredAt: Timestamp en formato ISO 8601 UTC.
 *    - correlationId: Identificador de correlación (activationId) que une de punta a punta toda la saga.
 *    - customerId: Identificador del cliente que actúa como clave de partición en Kafka para orden estricto.
 *    - source: Nombre del microservicio emisor (activation-api, billing-service, etc.).
 *    - payload: Contenido fuertemente tipado según el tipo de evento.
 * 3. Se agregó la función factoría `createEventEnvelope` que estandariza la creación de sobres con
 *    UUIDs automáticos y marcas de tiempo UTC.
 * =================================================================================================
 */

export interface EventEnvelope<T = Record<string, any>> {
  eventId: string;
  eventType: string;
  version: number;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
  payload: T;
}

/**
 * Parámetros requeridos para construir un sobre de evento estándar
 */
export interface CreateEventParams<T> {
  eventType: string;
  correlationId: string;
  customerId: string;
  source: string;
  payload: T;
  version?: number;
  eventId?: string;
}

/**
 * Función helper para construir instancias consistentes de sobres de eventos
 */
export function createEventEnvelope<T>(params: CreateEventParams<T>): EventEnvelope<T> {
  return {
    eventId: params.eventId || (typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substring(2) + Date.now().toString(36)),
    eventType: params.eventType,
    version: params.version ?? 1,
    occurredAt: new Date().toISOString(),
    correlationId: params.correlationId,
    customerId: params.customerId,
    source: params.source,
    payload: params.payload,
  };
}
