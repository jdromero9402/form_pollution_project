FROM node:18-alpine AS base
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH

FROM base AS deps
COPY package.json package-lock.json ./
RUN npm ci --silent

FROM deps AS dev
COPY . .
EXPOSE 3000