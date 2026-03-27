# Use the official Node.js image
FROM node:20

# Create app directory
WORKDIR /usr/src/app

# Copy package files first (for better caching)
COPY server/package*.json ./

# Install dependencies
RUN npm ci

# Copy the rest of your server and client code
COPY server/ ./server/
COPY client/ ./client/

# Expose the port your Express app runs on (e.g., 3000)
EXPOSE 5555

# Start the application
CMD [ "node", "server/app.js" ]