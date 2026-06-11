# =====================================================================
# SUSForge — Frontend Next.js (App Router) em modo desenvolvimento
# ---------------------------------------------------------------------
# Imagem leve baseada em Node 20 Alpine. Em dev:
#   - bind mount do código (./frontend → /app) para hot reload
#   - node_modules e .next preservados em volumes nomeados
#
# Para build de produção, trocar CMD por `npm run build && npm run start`
# e remover o bind mount no compose.
# =====================================================================

FROM node:20-alpine

WORKDIR /app

# Toolchain mínima — algumas deps nativas (ex.: pg) precisam de python
# durante o npm install para builds opcionais.
RUN apk add --no-cache libc6-compat python3 make g++

# Copia manifests primeiro para aproveitar cache de layers
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install --no-audit --no-fund

# Código fonte (sobrescrito pelo bind mount em dev)
COPY frontend/ ./

ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

EXPOSE 3000

CMD ["npm", "run", "dev"]
