FROM node:20-alpine

WORKDIR /app

# Copy dependency files
COPY package*.json ./

# Install dependencies
RUN npm ci --omit=dev

# Copy application source code
COPY . .

# Default command runs the API server
EXPOSE 3000
CMD ["node", "src/server.js"]
