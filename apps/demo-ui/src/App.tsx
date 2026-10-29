/**
 * =================================================================================================
 * ARCHIVO: apps/demo-ui/src/App.tsx
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Componente principal de la interfaz de usuario en React para la POC (RF-01, RF-08).
 * 2. Conexión WebSocket en vivo mediante `socket.io-client` al backend `activation-api` (puerto 3000):
 *    - Escucha `activation:updated`: actualiza el estado de la saga en pantalla sin recargar la página.
 *    - Escucha `activation:event`: agrega los eventos en tiempo real a la timeline cronológica.
 * 3. Formulario "Contratar plan" (RF-01):
 *    - Input para `customerId` (clave de partición).
 *    - Selector de planes comerciales.
 *    - Selector de fallos forzados ('none', 'billing', 'provisioning').
 *    - Botones de acceso rápido para los escenarios del guión de demo (1, 2, etc.).
 * 4. Timeline en vivo (RF-08):
 *    - Muestra los 4 estados de la máquina de estados: PENDING -> IN_PROGRESS -> ACTIVE o FAILED.
 *    - Destaca la compensación de la saga con distintivos visuales (`BillingAccountCancelled`).
 *    - Inspector interactivo del payload de cada sobre (EventEnvelope).
 * 5. Barra de accesos directos a Kafka UI (8080), Mailhog (8025) y la API REST.
 * =================================================================================================
 */

import React, { useState, useEffect } from 'react';
import { io, Socket } from 'socket.io-client';
import {
  Layers,
  Radio,
  Mail,
  ExternalLink,
  CheckCircle2,
  AlertTriangle,
  Clock,
  RefreshCw,
  Send,
  Zap,
  Server,
  Database,
  ArrowRight,
  ShieldCheck,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';

interface HistoryItem {
  eventType: string;
  at: string;
  source?: string;
  details?: any;
}

interface StepResult {
  status: 'OK' | 'FAILED';
  at: string;
  details?: any;
  reason?: string;
}

interface ActivationItem {
  _id: string;
  customerId: string;
  planId: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
  simulateFailure: 'none' | 'billing' | 'provisioning';
  steps: {
    billing?: StepResult | null;
    provisioning?: StepResult | null;
  };
  history: HistoryItem[];
  createdAt: string;
  updatedAt: string;
}

const API_BASE = 'http://localhost:3000';

export default function App() {
  const [activations, setActivations] = useState<ActivationItem[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [customerId, setCustomerId] = useState<string>('C-1001');
  const [planId, setPlanId] = useState<string>('Fibra Óptica 500 MB Simétrica');
  const [simulateFailure, setSimulateFailure] = useState<'none' | 'billing' | 'provisioning'>('none');
  const [loading, setLoading] = useState<boolean>(false);
  const [socketConnected, setSocketConnected] = useState<boolean>(false);
  const [expandedPayloads, setExpandedPayloads] = useState<Record<number, boolean>>({});

  // Carga inicial de activaciones
  const fetchActivations = async () => {
    try {
      const res = await fetch(`${API_BASE}/activations`);
      if (res.ok) {
        const data = await res.json();
        setActivations(data);
        if (data.length > 0 && !selectedId) {
          setSelectedId(data[0]._id);
        }
      }
    } catch (e) {
      console.warn('Backend aún no disponible o cargando...');
    }
  };

  useEffect(() => {
    fetchActivations();

    // Conexión por WebSocket a activation-api
    const socket: Socket = io(API_BASE, {
      transports: ['websocket', 'polling'],
      reconnectionAttempts: 15,
      reconnectionDelay: 2000,
    });

    socket.on('connect', () => {
      setSocketConnected(true);
    });

    socket.on('disconnect', () => {
      setSocketConnected(false);
    });

    // Actualización de estado de la activación
    socket.on('activation:updated', (updated: ActivationItem) => {
      setActivations((prev) => {
        const exists = prev.some((a) => a._id === updated._id);
        if (exists) {
          return prev.map((a) => (a._id === updated._id ? updated : a));
        } else {
          return [updated, ...prev];
        }
      });
      setSelectedId((prev) => prev || updated._id);
    });

    // Eventos individuales de la timeline
    socket.on('activation:event', (payload: { activationId: string; event: any }) => {
      setActivations((prev) =>
        prev.map((act) => {
          if (act._id === payload.activationId) {
            const historyCopy = [...(act.history || [])];
            const alreadyHas = historyCopy.some(
              (h) => h.eventType === payload.event.eventType && h.at === payload.event.occurredAt,
            );
            if (!alreadyHas) {
              historyCopy.push({
                eventType: payload.event.eventType,
                at: payload.event.occurredAt || new Date().toISOString(),
                source: payload.event.source,
                details: payload.event.payload,
              });
            }
            return { ...act, history: historyCopy };
          }
          return act;
        }),
      );
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  const handleCreate = async (failMode = simulateFailure, client = customerId) => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerId: client,
          planId,
          simulateFailure: failMode,
        }),
      });

      if (res.status === 202) {
        const created = await res.json();
        setSelectedId(created.activationId);
        // Incrementar cliente sugerido
        const num = parseInt(client.replace(/\D/g, '') || '1000', 10);
        setCustomerId(`C-${num + 1}`);
      }
    } catch (e) {
      alert('Error conectando con activation-api: ' + e);
    } finally {
      setLoading(false);
    }
  };

  const selectedActivation = activations.find((a) => a._id === selectedId) || activations[0];

  const togglePayload = (idx: number) => {
    setExpandedPayloads((prev) => ({ ...prev, [idx]: !prev[idx] }));
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'ACTIVE':
        return '#10b981'; // green
      case 'FAILED':
        return '#ef4444'; // red
      case 'IN_PROGRESS':
        return '#3b82f6'; // blue
      case 'PENDING':
      default:
        return '#f59e0b'; // amber
    }
  };

  return (
    <div style={{ maxWidth: '1440px', margin: '0 auto', padding: '24px', display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {/* HEADER DE LA POC */}
      <header style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        background: '#111827',
        padding: '20px 24px',
        borderRadius: '12px',
        border: '1px solid #1f2937',
        boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.3)'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          <div style={{
            background: 'linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%)',
            padding: '12px',
            borderRadius: '10px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center'
          }}>
            <Layers size={28} color="#ffffff" />
          </div>
          <div>
            <h1 style={{ fontSize: '20px', fontWeight: '700', letterSpacing: '-0.02em', color: '#f9fafb' }}>
              POC Activación de Servicios con Kafka
            </h1>
            <p style={{ fontSize: '13px', color: '#9ca3af' }}>
              Ingeniería en Sistemas de Información · Sistemas Distribuidos 2026 · Saga Coreografiada
            </p>
          </div>
        </div>

        {/* ACCESOS DIRECTOS DE OBSERVABILIDAD */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            background: '#1e293b',
            padding: '6px 12px',
            borderRadius: '20px',
            fontSize: '12px',
            border: '1px solid #334155'
          }}>
            <Radio size={14} color={socketConnected ? '#10b981' : '#ef4444'} className={socketConnected ? 'pulsing' : ''} />
            <span>WebSocket: {socketConnected ? 'En vivo' : 'Desconectado'}</span>
          </div>

          <a
            href="http://localhost:8080"
            target="_blank"
            rel="noreferrer"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              background: '#1e293b',
              color: '#60a5fa',
              textDecoration: 'none',
              padding: '6px 12px',
              borderRadius: '6px',
              fontSize: '13px',
              fontWeight: '500',
              border: '1px solid #334155'
            }}
          >
            <Server size={14} />
            Kafka UI (8080)
            <ExternalLink size={12} />
          </a>

          <a
            href="http://localhost:8025"
            target="_blank"
            rel="noreferrer"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              background: '#1e293b',
              color: '#34d399',
              textDecoration: 'none',
              padding: '6px 12px',
              borderRadius: '6px',
              fontSize: '13px',
              fontWeight: '500',
              border: '1px solid #334155'
            }}
          >
            <Mail size={14} />
            Mailhog (8025)
            <ExternalLink size={12} />
          </a>
        </div>
      </header>

      {/* CONTENIDO PRINCIPAL: 2 COLUMNAS */}
      <div style={{ display: 'grid', gridTemplateColumns: '400px 1fr', gap: '20px' }}>
        
        {/* PANEL IZQUIERDO: FORMULARIO Y ESCENARIOS */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {/* Formulario de Contratación */}
          <div style={{
            background: '#111827',
            padding: '20px',
            borderRadius: '12px',
            border: '1px solid #1f2937'
          }}>
            <h2 style={{ fontSize: '16px', fontWeight: '600', marginBottom: '16px', display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Zap size={18} color="#3b82f6" />
              Contratar Nuevo Plan
            </h2>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
              <div>
                <label style={{ display: 'block', fontSize: '12px', color: '#9ca3af', marginBottom: '6px' }}>
                  ID Cliente (Clave de Partición Kafka - customerId):
                </label>
                <input
                  type="text"
                  value={customerId}
                  onChange={(e) => setCustomerId(e.target.value)}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    background: '#1e293b',
                    border: '1px solid #334155',
                    borderRadius: '6px',
                    color: '#f9fafb',
                    fontSize: '14px',
                  }}
                />
              </div>

              <div>
                <label style={{ display: 'block', fontSize: '12px', color: '#9ca3af', marginBottom: '6px' }}>
                  Plan a Contratar:
                </label>
                <select
                  value={planId}
                  onChange={(e) => setPlanId(e.target.value)}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    background: '#1e293b',
                    border: '1px solid #334155',
                    borderRadius: '6px',
                    color: '#f9fafb',
                    fontSize: '14px',
                  }}
                >
                  <option value="Fibra Óptica 500 MB Simétrica">Fibra Óptica 500 MB Simétrica</option>
                  <option value="Plan Conectividad 5G Max">Plan Conectividad 5G Max</option>
                  <option value="Pack Dúo Hogar 1 GB + TV">Pack Dúo Hogar 1 GB + TV</option>
                </select>
              </div>

              <div>
                <label style={{ display: 'block', fontSize: '12px', color: '#9ca3af', marginBottom: '6px' }}>
                  Simulación de Fallos (RF-01):
                </label>
                <select
                  value={simulateFailure}
                  onChange={(e) => setSimulateFailure(e.target.value as any)}
                  style={{
                    width: '100%',
                    padding: '8px 12px',
                    background: '#1e293b',
                    border: '1px solid #334155',
                    borderRadius: '6px',
                    color: '#f9fafb',
                    fontSize: '14px',
                  }}
                >
                  <option value="none">Ninguno (Camino feliz - Éxito total)</option>
                  <option value="provisioning">Forzar Fallo en Provisioning (Dispara Compensación)</option>
                  <option value="billing">Forzar Fallo en Billing</option>
                </select>
              </div>

              <button
                onClick={() => handleCreate()}
                disabled={loading}
                style={{
                  marginTop: '8px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  gap: '8px',
                  background: '#2563eb',
                  color: '#ffffff',
                  border: 'none',
                  padding: '10px 16px',
                  borderRadius: '6px',
                  fontSize: '14px',
                  fontWeight: '600',
                  cursor: loading ? 'not-allowed' : 'pointer',
                  opacity: loading ? 0.7 : 1,
                  transition: 'background 0.2s',
                }}
              >
                {loading ? <RefreshCw size={16} className="pulsing" /> : <Send size={16} />}
                Disparar POST /activations
              </button>
            </div>
          </div>

          {/* Botones de Escenarios de Demo Rápidos */}
          <div style={{
            background: '#111827',
            padding: '20px',
            borderRadius: '12px',
            border: '1px solid #1f2937'
          }}>
            <h2 style={{ fontSize: '14px', fontWeight: '600', marginBottom: '12px', color: '#9ca3af' }}>
              Guión de Demo (Sección 10):
            </h2>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
              <button
                onClick={() => handleCreate('none')}
                style={{
                  textAlign: 'left',
                  padding: '10px 12px',
                  background: '#1e293b',
                  border: '1px solid #334155',
                  borderRadius: '6px',
                  color: '#f9fafb',
                  fontSize: '13px',
                  cursor: 'pointer',
                }}
              >
                <strong style={{ color: '#10b981' }}>#1 Camino Feliz:</strong> Plan sin fallo (4 eventos en verde + Email en Mailhog)
              </button>

              <button
                onClick={() => handleCreate('provisioning')}
                style={{
                  textAlign: 'left',
                  padding: '10px 12px',
                  background: '#1e293b',
                  border: '1px solid #334155',
                  borderRadius: '6px',
                  color: '#f9fafb',
                  fontSize: '13px',
                  cursor: 'pointer',
                }}
              >
                <strong style={{ color: '#ef4444' }}>#2 Fallo y Compensación:</strong> Fallo en provisioning y anulación en billing
              </button>

              <button
                onClick={() => handleCreate('billing')}
                style={{
                  textAlign: 'left',
                  padding: '10px 12px',
                  background: '#1e293b',
                  border: '1px solid #334155',
                  borderRadius: '6px',
                  color: '#f9fafb',
                  fontSize: '13px',
                  cursor: 'pointer',
                }}
              >
                <strong style={{ color: '#f59e0b' }}>#2b Fallo en Facturación:</strong> Error forzado en cobro bancario
              </button>
            </div>
          </div>

          {/* Historial de Activaciones */}
          <div style={{
            background: '#111827',
            padding: '20px',
            borderRadius: '12px',
            border: '1px solid #1f2937',
            flex: 1,
            maxHeight: '380px',
            overflowY: 'auto',
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
              <h2 style={{ fontSize: '14px', fontWeight: '600', color: '#9ca3af' }}>
                Activaciones Registradas ({activations.length})
              </h2>
              <button
                onClick={fetchActivations}
                style={{ background: 'transparent', border: 'none', color: '#60a5fa', cursor: 'pointer' }}
              >
                <RefreshCw size={14} />
              </button>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
              {activations.map((item) => (
                <div
                  key={item._id}
                  onClick={() => setSelectedId(item._id)}
                  style={{
                    padding: '10px 12px',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    background: selectedId === item._id ? '#1e293b' : '#0f172a',
                    border: selectedId === item._id ? '1px solid #3b82f6' : '1px solid #1e293b',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                  }}
                >
                  <div>
                    <div style={{ fontWeight: '600', fontSize: '13px' }}>{item._id}</div>
                    <div style={{ fontSize: '11px', color: '#9ca3af' }}>{item.customerId} · {item.planId}</div>
                  </div>
                  <span
                    style={{
                      fontSize: '11px',
                      fontWeight: '700',
                      padding: '2px 8px',
                      borderRadius: '12px',
                      background: `${getStatusColor(item.status)}20`,
                      color: getStatusColor(item.status),
                      border: `1px solid ${getStatusColor(item.status)}`,
                    }}
                  >
                    {item.status}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* PANEL DERECHO: TIMELINE EN VIVO Y DETALLE DE LA SAGA */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {selectedActivation ? (
            <div style={{
              background: '#111827',
              padding: '24px',
              borderRadius: '12px',
              border: '1px solid #1f2937',
              minHeight: '650px',
            }}>
              {/* Encabezado del Detalle */}
              <div style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'flex-start',
                paddingBottom: '20px',
                borderBottom: '1px solid #1f2937',
                marginBottom: '24px',
              }}>
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '6px' }}>
                    <h2 style={{ fontSize: '22px', fontWeight: '700' }}>
                      Activación: {selectedActivation._id}
                    </h2>
                    <span
                      style={{
                        fontSize: '13px',
                        fontWeight: '700',
                        padding: '4px 12px',
                        borderRadius: '20px',
                        background: `${getStatusColor(selectedActivation.status)}20`,
                        color: getStatusColor(selectedActivation.status),
                        border: `1px solid ${getStatusColor(selectedActivation.status)}`,
                      }}
                    >
                      {selectedActivation.status}
                    </span>
                  </div>
                  <p style={{ fontSize: '14px', color: '#9ca3af' }}>
                    Cliente: <strong>{selectedActivation.customerId}</strong> | Plan: <strong>{selectedActivation.planId}</strong>
                    {selectedActivation.simulateFailure !== 'none' && (
                      <span style={{ color: '#ef4444', marginLeft: '12px' }}>
                        (Modo Fallo Forzado: {selectedActivation.simulateFailure})
                      </span>
                    )}
                  </p>
                </div>

                {/* Resumen de Pasos Paralelos (RF-04) */}
                <div style={{ display: 'flex', gap: '16px' }}>
                  <div style={{
                    padding: '8px 16px',
                    borderRadius: '8px',
                    background: '#1e293b',
                    border: '1px solid #334155',
                    textAlign: 'center',
                  }}>
                    <div style={{ fontSize: '11px', color: '#9ca3af' }}>Facturación (billing)</div>
                    <div style={{
                      fontWeight: '700',
                      fontSize: '13px',
                      color: selectedActivation.steps?.billing?.status === 'OK' ? '#10b981' : selectedActivation.steps?.billing?.status === 'FAILED' ? '#ef4444' : '#f59e0b',
                    }}>
                      {selectedActivation.steps?.billing?.status || 'PENDIENTE'}
                    </div>
                  </div>

                  <div style={{
                    padding: '8px 16px',
                    borderRadius: '8px',
                    background: '#1e293b',
                    border: '1px solid #334155',
                    textAlign: 'center',
                  }}>
                    <div style={{ fontSize: '11px', color: '#9ca3af' }}>Aprovisionamiento (prov)</div>
                    <div style={{
                      fontWeight: '700',
                      fontSize: '13px',
                      color: selectedActivation.steps?.provisioning?.status === 'OK' ? '#10b981' : selectedActivation.steps?.provisioning?.status === 'FAILED' ? '#ef4444' : '#f59e0b',
                    }}>
                      {selectedActivation.steps?.provisioning?.status || 'PENDIENTE'}
                    </div>
                  </div>
                </div>
              </div>

              {/* TIMELINE DE EVENTOS KAFKA (RF-08) */}
              <h3 style={{ fontSize: '16px', fontWeight: '600', marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <Clock size={18} color="#3b82f6" />
                Línea de Tiempo de Eventos en Kafka (Tiempo Real por WebSocket)
              </h3>

              <div style={{ position: 'relative', paddingLeft: '32px' }}>
                {/* Línea vertical de fondo */}
                <div style={{
                  position: 'absolute',
                  left: '11px',
                  top: '8px',
                  bottom: '24px',
                  width: '2px',
                  background: '#374151',
                }} />

                <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
                  {(selectedActivation.history || []).map((ev, index) => {
                    const isFailure = ev.eventType.includes('Failed');
                    const isCompleted = ev.eventType.includes('Completed');
                    const isCancelled = ev.eventType.includes('Cancelled');

                    let iconColor = '#3b82f6';
                    if (isCompleted) iconColor = '#10b981';
                    if (isFailure) iconColor = '#ef4444';
                    if (isCancelled) iconColor = '#f59e0b';

                    const isExpanded = expandedPayloads[index];

                    return (
                      <div key={index} style={{ position: 'relative' }}>
                        {/* Nodo de la timeline */}
                        <div style={{
                          position: 'absolute',
                          left: '-32px',
                          top: '12px',
                          width: '22px',
                          height: '22px',
                          borderRadius: '50%',
                          background: '#111827',
                          border: `2px solid ${iconColor}`,
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          zIndex: 2,
                        }}>
                          <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: iconColor }} />
                        </div>

                        {/* Tarjeta del evento */}
                        <div style={{
                          background: '#1e293b',
                          borderRadius: '8px',
                          padding: '14px 18px',
                          border: isCancelled ? '1px dashed #f59e0b' : '1px solid #334155',
                        }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                              <span style={{ fontSize: '15px', fontWeight: '700', color: iconColor }}>
                                {ev.eventType}
                              </span>
                              {ev.source && (
                                <span style={{
                                  fontSize: '11px',
                                  padding: '2px 8px',
                                  background: '#0f172a',
                                  color: '#94a3b8',
                                  borderRadius: '4px',
                                  border: '1px solid #334155',
                                }}>
                                  origen: {ev.source}
                                </span>
                              )}
                              {isCancelled && (
                                <span style={{
                                  fontSize: '11px',
                                  fontWeight: '700',
                                  padding: '2px 8px',
                                  background: '#f59e0b20',
                                  color: '#f59e0b',
                                  borderRadius: '4px',
                                  border: '1px solid #f59e0b',
                                }}>
                                  COMPENSACIÓN EJECUTADA
                                </span>
                              )}
                            </div>
                            <span style={{ fontSize: '12px', color: '#9ca3af' }}>
                              {new Date(ev.at).toLocaleTimeString()} ({new Date(ev.at).toISOString().substring(11, 19)})
                            </span>
                          </div>

                          {/* Botón para expandir/colapsar payload */}
                          {ev.details && (
                            <div style={{ marginTop: '8px' }}>
                              <button
                                onClick={() => togglePayload(index)}
                                style={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '4px',
                                  background: 'transparent',
                                  border: 'none',
                                  color: '#60a5fa',
                                  fontSize: '12px',
                                  cursor: 'pointer',
                                  padding: '0',
                                }}
                              >
                                {isExpanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                                {isExpanded ? 'Ocultar payload' : 'Ver payload del evento'}
                              </button>

                              {isExpanded && (
                                <pre style={{
                                  marginTop: '8px',
                                  padding: '10px 14px',
                                  background: '#0b0f19',
                                  borderRadius: '6px',
                                  fontSize: '12px',
                                  color: '#cbd5e1',
                                  overflowX: 'auto',
                                  border: '1px solid #1e293b',
                                }}>
                                  {JSON.stringify(ev.details, null, 2)}
                                </pre>
                              )}
                            </div>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
          ) : (
            <div style={{
              background: '#111827',
              padding: '40px',
              borderRadius: '12px',
              textAlign: 'center',
              color: '#9ca3af',
              border: '1px solid #1f2937',
            }}>
              No hay activaciones registradas todavía. Usa el formulario a la izquierda para crear la primera activación.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
