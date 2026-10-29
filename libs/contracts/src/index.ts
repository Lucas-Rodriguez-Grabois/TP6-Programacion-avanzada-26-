/**
 * =================================================================================================
 * ARCHIVO: libs/contracts/src/index.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Punto de entrada y re-exportación del módulo `@poc/contracts`.
 * 2. Exporta todos los sobres (EventEnvelope), tópicos (TOPICS, DLQ_TOPICS, CONSUMER_GROUPS)
 *    y los contratos y tipos de eventos fuertemente tipados.
 * 3. Actúa como la única fuente de la verdad para todos los microservicios y el frontend.
 * =================================================================================================
 */

export * from './envelope';
export * from './topics';
export * from './events';
