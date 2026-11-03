FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY analytics_service/ ./analytics_service/

ENV PYTHONPATH=/app
ENV PORT=8002
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=orders-events

EXPOSE 8002

CMD ["uvicorn", "analytics_service.app:app", "--host", "0.0.0.0", "--port", "8002"]
