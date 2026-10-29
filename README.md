<!--
=================================================================================================
ARCHIVO: README.md
=================================================================================================
DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
1. Documento central y guía de inicio rápido para el Trabajo Práctico Nº 6 (TP6): POC Activación de Servicios con Kafka.
2. Contiene la explicación de la arquitectura monorepo, instrucciones de compilación, ejecución con Docker Compose,
   y guía rápida para reproducir los 5 escenarios del guión de demo.
3. Lista exhaustiva de todos los requerimientos funcionales (RF-01 a RF-10) y no funcionales (RNF-01 a RNF-07)
   implementados y verificados en el código.
=================================================================================================
-->

# TP6: POC Activación de Servicios con Apache Kafka

**Asignatura:** Sistemas Distribuidos / Programación Avanzada - 2026  
**Tecnologías:** Apache Kafka (KRaft), NestJS (`@nestjs/microservices`), MongoDB (Replica Set), React + Vite, Mailhog, Docker Compose.

---

## 📋 Resumen del Proyecto

Este proyecto implementa la solución completa solicitada en la consigna del TP6 para la activación de servicios de telecomunicaciones mediante una **Saga Coreografiada con Agregador** sobre **Apache Kafka**.

Se elimina cualquier acoplamiento síncrono entre servicios: la contratación de un plan desde la aplicación web emite un evento `ActivationRequested`, disparando en paralelo la facturación (`billing-service`) y el aprovisionamiento de red (`provisioning-service`). El agregador (`activation-api`) coordina el resultado: si ambos aprueban, la activación concluye exitosamente emitiendo correo de bienvenida; si alguno falla, se desencadena la **compensación reactiva** anulando automáticamente la cuenta creada en facturación.

---

## 🏗️ Estructura del Monorepo

```
activation-poc/ (TP6)
├── apps/
│   ├── activation-api/          # REST API + WebSocket Gateway + Agregador de Saga (Puerto 3000)
│   ├── billing-service/         # Facturación simulada, fallos forzados y compensación
│   ├── provisioning-service/    # Aprovisionamiento de red con retardo de 1-3 segundos
│   ├── notification-service/    # Envío de emails transaccionales a Mailhog (SMTP)
│   ├── crm-analytics-service/   # Bitácora inmutable de auditoría (consume todos los tópicos)
│   ├── loyalty-service/         # Escenario 5: Replay del log histórico con auto.offset.reset=earliest
│   └── demo-ui/                 # Frontend en React + Vite + Socket.IO (Puerto 5173)
├── libs/
│   ├── contracts/               # Tipos de eventos, contratos canónicos, tópicos y sobre común
│   └── kafka-toolkit/           # Idempotencia, reintentos exponenciales, DLQ y Outbox transaccional
├── docker/
│   ├── Dockerfile.service       # Empaquetado multipropósito para los microservicios NestJS
│   └── create-topics.sh         # Script de creación de tópicos con 3 particiones y retención de 7 días
├── docs/
│   ├── architecture.md          # Especificación detallada de arquitectura y diagramas
│   └── demo-script.md           # Guión paso a paso de los 5 escenarios de demostración
├── docker-compose.yml           # Orquestación de infraestructura y microservicios
├── package.json                 # Monorepo NPM Workspaces
├── tsconfig.base.json           # Configuración base de TypeScript con alias de módulos
└── README.md                    # Documentación principal
```

---

## 🚀 Inicio Rápido con Docker Compose

El proyecto cumple con **RNF-01** (todo el entorno se levanta con un solo comando sin pasos manuales):

```bash
docker compose up --build
```

Una vez levantado el entorno, los servicios quedan disponibles en:
- **Demo UI (React):** [http://localhost:5173](http://localhost:5173)
- **Kafka UI (Kafbat):** [http://localhost:8080](http://localhost:8080)
- **Mailhog (Web UI):** [http://localhost:8025](http://localhost:8025)
- **API REST (activation-api):** [http://localhost:3000/activations](http://localhost:3000/activations)

---

## 🎯 Requerimientos Cubiertos

### Requerimientos Funcionales
- [x] **RF-01:** Formulario en React que permite indicar `customerId`, `planId` y modo de fallo forzado (`none`, `billing`, `provisioning`).
- [x] **RF-02:** `POST /activations` responde inmediatamente `202 Accepted` con `activationId` y estado `PENDING`.
- [x] **RF-03:** `activation-api` publica `ActivationRequested` en Kafka con clave `customerId`.
- [x] **RF-04:** `billing-service` y `provisioning-service` procesan el evento en paralelo de forma asíncrona.
- [x] **RF-05:** Si ambos confirman OK, la activación pasa a `ACTIVE` y se publica `ActivationCompleted`.
- [x] **RF-06:** Si alguno falla, pasa a `FAILED`, se emite `ActivationFailed` y `billing` anula la cuenta (`BillingAccountCancelled`).
- [x] **RF-07:** `notification-service` despacha el correo correspondiente a Mailhog según sea éxito o fallo.
- [x] **RF-08:** La UI refleja la timeline de eventos en tiempo real mediante WebSockets (`socket.io`).
- [x] **RF-09:** `GET /activations/:id` devuelve el estado consolidado y el historial completo.
- [x] **RF-10:** `crm-analytics-service` almacena la copia de todos los eventos del cluster.

### Requerimientos No Funcionales
- [x] **RNF-01:** Todo el cluster y las aplicaciones levantan con `docker compose up`.
- [x] **RNF-02:** Clave de partición = `customerId` para garantizar orden estricto de eventos por cliente.
- [x] **RNF-03:** Idempotencia en cada consumidor mediante colección `processed_events` e índice único en `eventId`.
- [x] **RNF-04:** Reintentos (1s, 2s, 4s) y desvío a `<topic>.dlq` con headers técnicos ante fallos persistentes.
- [x] **RNF-05:** Retención de offsets: Si un servicio cae y vuelve, procesa los eventos pendientes sin pérdidas.
- [x] **RNF-06:** Identificador de correlación `correlationId` en todos los sobres de eventos para rastreabilidad integral.
- [x] **RNF-07:** Contratos compartidos en librería `@poc/contracts` con esquema versionado (`version: 1`).

---

## 🧪 Ejecución de los 5 Escenarios de Demostración

Consulte la guía exhaustiva en [`docs/demo-script.md`](file:///docs/demo-script.md) para ejecutar:
1. **Camino Feliz:** Contratación sin fallo -> 4 eventos en verde + Email de bienvenida en Mailhog.
2. **Fallo y Compensación:** Fallo forzado en provisioning -> Anulación automática en billing y email de aviso.
3. **Servicio Caído:** `docker compose stop notification-service` -> Generación de activaciones -> Verificación de Lag en Kafka UI -> `docker compose start notification-service` -> Recepción íntegra de emails pendientes.
4. **Escalar Billing:** `docker compose up -d --scale billing-service=2` -> Reparto equilibrado de particiones en Kafka UI.
5. **Replay del Log Histórico:** `docker compose --profile loyalty up -d loyalty-service` -> Procesamiento desde offset 0 con acumulación de puntos de fidelidad.
