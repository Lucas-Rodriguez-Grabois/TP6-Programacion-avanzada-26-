/**
 * =================================================================================================
 * ARCHIVO: apps/demo-ui/vite.config.ts
 * =================================================================================================
 * DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
 * 1. Configuración de Vite para el empaquetado del frontend en React.
 * 2. Habilita el plugin oficial de React y define el puerto local en 5173 según la sección 10 del PDF.
 * 3. Habilita `--host 0.0.0.0` para despliegue y exposición limpia en contenedores Docker Compose.
 * =================================================================================================
 */

import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    host: true,
  },
});
