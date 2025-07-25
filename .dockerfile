# Используем официальный Python-образ
FROM python:3.11-slim

# Установка системных зависимостей (для pandas, matplotlib и др.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории внутри контейнера
WORKDIR /app

# Копирование зависимостей
COPY requirements.txt .

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование всего проекта
COPY . .

# Установка переменных окружения
ENV PYTHONUNBUFFERED=1

# Открытие порта для FastAPI
EXPOSE 8000

# Команда запуска FastAPI
CMD ["uvicorn", "multi_app:app", "--host", "0.0.0.0", "--port", "8000"]
