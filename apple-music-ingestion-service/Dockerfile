FROM python:3.12-slim

WORKDIR /app

COPY ../requirements.txt requirements.txt 

COPY ../.env .env

RUN pip install --no-cache-dir -r requirements.txt

COPY apple-music-ingestion-service/ . 

CMD ["python", "app.py"]
