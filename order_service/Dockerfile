FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY order_service/ ./order_service/

ENV PYTHONPATH=/app
ENV PORT=8001
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=orders-events
ENV ANALYTICS_SERVICE_URL=http://analytics-service:8002/internal/events

EXPOSE 8001

CMD ["uvicorn", "order_service.app:app", "--host", "0.0.0.0", "--port", "8001"]
