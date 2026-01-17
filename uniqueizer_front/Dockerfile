# Этап 1: Сборка приложения
FROM node:20-alpine AS builder

WORKDIR /app

# Копируем package файлы
COPY package*.json ./

# Устанавливаем зависимости
RUN npm install --production

# Копируем исходный код
COPY . .

# Собираем production версию
RUN npm run build

# Этап 2: Production образ с nginx
FROM nginx:alpine

# Копируем собранное приложение из builder stage
# Проверьте, куда ваш фронтенд собирается (dist или build)
COPY --from=builder /app/dist /usr/share/nginx/html
COPY --from=builder /app/build /usr/share/nginx/html

# Копируем кастомную конфигурацию nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Открываем порт
EXPOSE 3721

# Запускаем nginx
CMD ["nginx", "-g", "daemon off;"]
