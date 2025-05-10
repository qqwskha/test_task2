# Этап 1: Сборка зависимостей Python
FROM python:3.9-slim AS builder

# Установка зависимостей Python
COPY requirements.txt /app/
WORKDIR /app
RUN pip3 install --user -r requirements.txt

# Этап 2: Финальный образ
FROM apache/spark:3.5.5

# Добавление Spark в PATH
ENV PATH=/opt/spark/bin:$PATH

# Копирование установленных зависимостей Python
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# Копирование кода
COPY . /app/
WORKDIR /app

# Запуск приложения
CMD ["spark-submit", "main.py"]