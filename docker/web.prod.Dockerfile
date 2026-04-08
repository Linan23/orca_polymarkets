FROM node:22-bookworm-slim AS build

WORKDIR /workspace/my-app

COPY my-app/package*.json ./
RUN npm ci

COPY my-app/ ./

ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

RUN npm run build

FROM caddy:2.10-alpine

COPY docker/Caddyfile /etc/caddy/Caddyfile
COPY --from=build /workspace/my-app/dist /srv

