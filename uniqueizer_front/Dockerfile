FROM nginx:alpine

# Копируем HTML и favicon
COPY index.html /usr/share/nginx/html/
COPY favicon.ico /usr/share/nginx/html/
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3721

CMD ["nginx", "-g", "daemon off;"]
