FROM nginx:alpine

# Копируем статические файлы
COPY . /usr/share/nginx/html

# Копируем nginx конфигурацию
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3721

CMD ["nginx", "-g", "daemon off;"]
