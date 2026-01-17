FROM python:3.13-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости и FFmpeg
RUN apt-get update && apt-get install -y \
    ffmpeg \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Проверяем установку FFmpeg
RUN ffmpeg -version

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY . .

# Создаем директории для загрузок и вывода
RUN mkdir -p /app/uploads /app/outputs && \
    chmod 777 /app/uploads /app/outputs

# Открываем порт
EXPOSE 5847

# Запускаем с uvicorn (БЕЗ gunicorn)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5847", "--workers", "4"]
