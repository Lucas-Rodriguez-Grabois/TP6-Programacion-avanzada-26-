/**
 * =================================================================================================
 * ARCHIVO: apps/activation-api/src/gateway/activation.gateway.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Implementación del WebSocket Gateway con Socket.IO para comunicación en tiempo real (RF-08).
 * 2. Emite eventos en vivo a la aplicación de Demo UI en React:
 *    - `activation:updated`: Notifica el cambio de estado completo del documento de activación.
 *    - `activation:event`: Transmite cada nuevo evento que llega a Kafka para renderizar la timeline al instante.
 * 3. Habilita CORS para permitir la conexión desde el puerto de la UI (5173 u otros).
 * 4. Gestiona la conexión y desconexión de clientes con logs informativos.
 * =================================================================================================
 */

import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';

@WebSocketGateway({
  cors: {
    origin: '*',
  },
})
export class ActivationGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(ActivationGateway.name);

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket) {
    this.logger.log(`[WEBSOCKET] Cliente conectado: ${client.id}`);
  }

  handleDisconnect(client: Socket) {
    this.logger.log(`[WEBSOCKET] Cliente desconectado: ${client.id}`);
  }

  @SubscribeMessage('ping')
  handlePing(client: Socket) {
    client.emit('pong', { time: new Date().toISOString() });
  }

  /**
   * Notifica a todos los clientes conectados sobre un cambio en la activación.
   */
  broadcastActivationUpdate(activation: any) {
    if (this.server) {
      this.server.emit('activation:updated', activation);
    }
  }

  /**
   * Notifica un nuevo evento individual recibido en la timeline de la saga.
   */
  broadcastTimelineEvent(eventData: { activationId: string; event: any }) {
    if (this.server) {
      this.server.emit('activation:event', eventData);
    }
  }
}
