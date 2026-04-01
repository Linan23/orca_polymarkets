FROM node:22-bookworm-slim

WORKDIR /workspace/my-app

COPY my-app/package*.json ./
RUN npm install
