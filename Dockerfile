# Используем лёгкий образ Python
FROM python:3.11-slim

# Рабочая директория
WORKDIR /app

# Устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код проекта
COPY . .

# Запускаем бота
CMD ["python", "bot.py"]
