# =================================================================================================
# ARCHIVO: apps/demo-ui/Dockerfile
# =================================================================================================
# DESCRIPCIÓN DE LO REALIZADO EN ESTE ARCHIVO:
# 1. Empaquetado en contenedor Docker para la aplicación frontend React + Vite + Socket.IO.
# 2. Expone el puerto estándar de Vite 5173 para acceso local desde el navegador del usuario.
# 3. Arranca el servidor de desarrollo de Vite con soporte de host abierto (`--host 0.0.0.0`)
#    permitiendo hot-reloading y comunicación sin fricción con `activation-api`.
# =================================================================================================

FROM node:20-alpine

WORKDIR /app

COPY apps/demo-ui/package*.json ./
RUN npm install

COPY apps/demo-ui/ ./

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host"]
