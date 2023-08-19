FROM node:16-slim

RUN mkdir -p /app
WORKDIR /app
COPY . /app

RUN npm ci
RUN npm run build

EXPOSE 3000