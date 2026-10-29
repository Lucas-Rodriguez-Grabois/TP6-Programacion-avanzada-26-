<!--
=================================================================================================
ARCHIVO: docs/demo-script.md
=================================================================================================
DESCRIPCIÓN DE LO REALIZADO EN ESTE DOCUMENTO:
1. Guión formal y detallado de demostración con los 5 escenarios requeridos en la Sección 10 del PDF.
2. Comandos exactos de Docker Compose y CLI para ejecutar cada prueba paso a paso.
3. Resultados esperados en cada interfaz:
   - Demo UI (http://localhost:5173)
   - Kafka UI (http://localhost:8080)
   - Mailhog Web (http://localhost:8025)
4. Explicación de los conceptos distribuidos demostrados: Fan-out, Sagas sin llamadas directas,
   Offsets y Lag, Balanceo de grupos de consumidores en particiones, y Replay histórico del log.
=================================================================================================
-->

# Guión de Demostración: 5 Escenarios de Arquitectura Kafka

Este documento detalla el paso a paso para ejecutar y verificar los **cinco escenarios** especificados en el documento de arquitectura de Sistemas Distribuidos 2026.

---

## Preparación de la Demostración

1. **Abrir el entorno en pantalla dividida (Split Screen):**
   - **Izquierda:** Demo UI (`http://localhost:5173`).
   - **Derecha:** Kafka UI (`http://localhost:8080`) y Mailhog (`http://localhost:8025`).
2. **Iniciar todos los servicios del cluster:**
   ```bash
   docker compose up --build
   ```
3. Esperar que los contenedores inicien y Kafka UI reporte el cluster `local-kraft` en estado **Online**.

---

## Escenario 1: Camino Feliz (Happy Path)

* **Concepto demostrado:** **Fan-out** (un único evento publicado dispara múltiples reacciones concurrentes en diferentes servicios sin llamadas punto a punto).
* **Acción:**
  1. En el formulario de la UI (`http://localhost:5173`), ingresar:
     - **Cliente:** `C-1001`
     - **Plan:** `Fibra Óptica 500 MB Simétrica`
     - **Modo de Fallo:** `Ninguno` (o click en el botón rápido `#1 Camino Feliz`).
  2. Presionar **Disparar POST /activations**.
* **Qué se observa:**
  - **En la Demo UI:**
    - La activación pasa inmediatamente a `PENDING` (código HTTP 202 Accepted).
    - La timeline en vivo muestra en tiempo real la llegada secuencial de los 4 eventos:
      1. `ActivationRequested` (emitido por `activation-api`)
      2. `BillingAccountCreated` (emitido por `billing-service`)
      3. `ProvisioningCompleted` (emitido por `provisioning-service` tras el retardo de 1.5s)
      4. `ActivationCompleted` (emitido por el agregador al constatar ambos pasos OK)
    - El estado final de la activación cambia a **`ACTIVE`** con distintivo verde.
  - **En Mailhog (`http://localhost:8025`):**
    - Aparece un nuevo correo dirigido a `c-1001@demo-telecom.com` con asunto:  
      `🎉 ¡Bienvenido! Tu plan Fibra Óptica 500 MB Simétrica está activo (Ref: act-XXXX)`.
  - **En Kafka UI (`http://localhost:8080`):**
    - En la sección **Topics**, los mensajes incrementan en `activation.requested`, `billing.events`, `provisioning.events` y `activation.events`. Todos con la clave `C-1001`.

---

## Escenario 2: Fallo en Aprovisionamiento y Compensación en Facturación

* **Concepto demostrado:** **Saga Coreografiada con Compensación Reactiva** (sin llamadas directas: un fallo en un servicio revierte los efectos ya consolidados en otro servicio mediante eventos compensatorios).
* **Acción:**
  1. En la UI, configurar:
     - **Cliente:** `C-1002`
     - **Plan:** `Plan Conectividad 5G Max`
     - **Modo de Fallo:** `Forzar Fallo en Provisioning` (o click en el botón `#2 Fallo y Compensación`).
  2. Presionar **Disparar POST /activations**.
* **Qué se observa:**
  - **En la Demo UI:**
    - Billing crea la cuenta simulada de inmediato (`BillingAccountCreated`).
    - Provisioning falla intencionalmente tras 1.5s y publica `ProvisioningFailed`.
    - `activation-api` detecta el fallo del paso de aprovisionamiento, marca la activación como **`FAILED`** y emite `ActivationFailed`.
    - `billing-service` consume `ActivationFailed` de `activation.events`, detecta que había creado la cuenta y ejecuta la **compensación**: cambia el estado de la cuenta a `CANCELLED` y emite `BillingAccountCancelled`.
    - La timeline resalta en amarillo y rojo el paso de compensación:
      `BillingAccountCancelled - COMPENSACIÓN EJECUTADA`.
  - **En Mailhog (`http://localhost:8025`):**
    - Llega un correo de aviso a `c-1002@demo-telecom.com` informando que la activación no pudo completarse debido a un fallo en provisioning y que los cargos fueron anulados.

---

## Escenario 3: Servicio Caído y Recuperación de Offset (Lag)

* **Concepto demostrado:** **Desacople temporal y retención de Offsets** (un servicio puede caerse temporalmente sin perder ningún mensaje; al volver en línea, retoma desde el último offset comiteado sin omisiones).
* **Acción:**
  1. Detener el microservicio de notificaciones desde la terminal:
     ```bash
     docker compose stop notification-service
     ```
  2. Crear **3 activaciones exitosas consecutivas** desde la Demo UI (ej: clientes `C-1003`, `C-1004`, `C-1005` en modo `Ninguno`).
  3. Ir a **Kafka UI** -> **Consumers** -> Grupo `notification-svc`.
     - Observar que el **Consumer Lag** marca **3** mensajes pendientes.
  4. Ir a **Mailhog**: Constatar que **NO** ha llegado ningún email para estos 3 clientes (el servicio de envío está apagado).
  5. Volver a encender el contenedor de notificaciones:
     ```bash
     docker compose start notification-service
     ```
* **Qué se observa:**
  - El contenedor arranca, se conecta a Kafka y lee su último committed offset.
  - El **Lag en Kafka UI desciende a 0**.
  - En **Mailhog** entran inmediatamente los **3 correos pendientes** correspondientes a las 3 activaciones creadas. **Ningún mensaje se perdió.**

---

## Escenario 4: Escalamiento Horizontal y Reparto de Particiones

* **Concepto demostrado:** **Consumer Groups y Particiones** (Kafka balancea automáticamente las particiones de un tópico entre las instancias activas de un mismo consumer group).
* **Acción:**
  1. En Kafka UI (`http://localhost:8080`), ingresar a **Consumers** -> `billing-svc`.
     - Notar que la única instancia activa tiene asignadas las **3 particiones** del tópico (0, 1 y 2).
  2. Escalar `billing-service` a **2 instancias concurrentes** ejecutando:
     ```bash
     docker compose up -d --scale billing-service=2
     ```
  3. Esperar 5 a 10 segundos a que ocurra el rebalanceo de Kafka (`session.timeout.ms`).
* **Qué se observa:**
  - En **Kafka UI** -> `billing-svc`:
    - Ahora aparecen **2 miembros (consumers)** dentro del mismo grupo.
    - Las 3 particiones se reparten de manera equilibrada (un consumidor atiende 2 particiones y el otro atiende 1).
    - Los mensajes de clientes se distribuyen entre ambas réplicas en función del hash del `customerId`, procesando en paralelo sin colisiones ni condiciones de carrera.

---

## Escenario 5: Nuevo Consumidor y Replay del Log Histórico

* **Concepto demostrado:** **Replay histórico inmutable** (se puede conectar un nuevo servicio con lógica de negocio diferente que lea todo el pasado desde el offset 0 con `auto.offset.reset = earliest`, sin modificar ni impactar a los microservicios existentes).
* **Acción:**
  1. Levantar el servicio `loyalty-service` utilizando el perfil `loyalty`:
     ```bash
     docker compose --profile loyalty up -d loyalty-service
     ```
  2. Inspeccionar los logs del nuevo contenedor:
     ```bash
     docker compose logs -f loyalty-service
     ```
* **Qué se observa:**
  - El servicio se conecta bajo un nuevo grupo: `loyalty-group`.
  - Lee desde el **offset 0** (`fromBeginning: true`) todos los eventos `ActivationCompleted` emitidos desde el inicio de la prueba.
  - En los logs se observa cómo reconstruye el balance de puntos para cada cliente:
    `⭐ [LOYALTY REPLAY] +100 puntos otorgados al cliente C-1001 por activación act-XXXX. Total: 100 pts`
    `⭐ [LOYALTY REPLAY] +100 puntos otorgados al cliente C-1003 por activación act-XXXX. Total: 100 pts`
  - Ninguno de los demás servicios (`activation-api`, `billing-service`, etc.) fue afectado ni recibió notificaciones innecesarias.

---

## Resumen de Comandos de Apoyo

```bash
# Iniciar todo el entorno
docker compose up --build

# Escenario 3: Detener y reiniciar notificaciones
docker compose stop notification-service
docker compose start notification-service

# Escenario 4: Escalar billing a 2 réplicas
docker compose up -d --scale billing-service=2

# Escenario 5: Levantar nuevo consumidor Loyalty
docker compose --profile loyalty up -d loyalty-service

# Detener y limpiar todo el entorno
docker compose down -v
```
