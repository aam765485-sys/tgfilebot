FROM node:20-slim
WORKDIR /app
COPY bot.js .
CMD ["node", "bot.js"]
