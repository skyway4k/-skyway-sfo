FROM node:20-slim
WORKDIR /app
COPY package.json .
RUN npm install
COPY server.js .
EXPOSE 8766
CMD ["node", "server.js"]
