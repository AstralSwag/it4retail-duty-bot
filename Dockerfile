FROM python:3.10-slim

ENV TZ=Europe/Moscow

RUN apt-get update && apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Создание директории для конфигов
RUN mkdir -p /app/config && chmod 777 /app/config

COPY . /app

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python", "bot.py"]