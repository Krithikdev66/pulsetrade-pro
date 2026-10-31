# Use official Node.js 20 LTS Alpine image for minimal image size & security
FROM node:20-alpine AS runner

WORKDIR /app

# Set production environment
ENV NODE_ENV=production
ENV PORT=3000

# Install dependencies first (layer caching)
COPY package*.json ./
RUN npm ci --omit=dev

# Copy application source code
COPY . .

# Set permissions for non-root user
USER node

# Expose default HTTP/WebSocket port
EXPOSE 3000

# Start server
CMD ["node", "server.js"]