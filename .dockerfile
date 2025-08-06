# Используем официальный Python-образ
FROM python:3.11-slim

# Установка системных зависимостей для pandas, playwright, matplotlib и т.д.
RUN apt-get update && apt-get install -y \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    libnss3 \
    libatk-bridge2.0-0 \
    libx11-xcb1 \
    libdrm2 \
    libgbm1 \
    libasound2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    wget \
    ca-certificates \
    fonts-liberation \
    libappindicator3-1 \
    libatk1.0-0 \
    libgtk-3-0 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории внутри контейнера
WORKDIR /app

# Копирование зависимостей
COPY requirements.txt .

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Установка браузеров Playwright
RUN playwright install --with-deps

# Копирование всего проекта
COPY . .

# Установка переменных окружения
ENV PYTHONUNBUFFERED=1

# Открытие порта для FastAPI
EXPOSE 8000

# Команда запуска FastAPI
CMD ["uvicorn", "multi_app:app", "--host", "0.0.0.0", "--port", "8000"]


