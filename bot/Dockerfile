FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package files first (better Docker layer caching)
COPY package*.json ./

# Install dependencies
RUN npm install --production

# Copy bot source
COPY index.js ./

# Fly.io sets PORT automatically
ENV PORT=8080

# Start the bot
CMD ["node", "index.js"]
