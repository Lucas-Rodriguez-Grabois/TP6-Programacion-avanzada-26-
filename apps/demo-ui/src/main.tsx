/**
 * =================================================================================================
 * ARCHIVO: apps/demo-ui/src/main.tsx
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Punto de montaje de la aplicación React 18 en el DOM (#root).
 * 2. Carga de los estilos globales CSS (`index.css`).
 * 3. Renderiza el componente principal `App`.
 * =================================================================================================
 */

import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
