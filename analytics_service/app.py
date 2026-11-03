import os
import sys
from typing import Dict, Any
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.kafka_bus import KafkaEventBus
from analytics_service.analytics import AnalyticsAggregator

SERVICE_PORT = int(os.getenv("PORT", "8002"))

app = FastAPI(
    title="Analytics Service (CQRS + Event Sourcing)",
    description="Сервис аналитики, агрегирующий метрики из топика Kafka (orders-events)",
    version="1.0.0"
)

aggregator = AnalyticsAggregator()
kafka_bus = KafkaEventBus(service_name="AnalyticsService")

@app.on_event("startup")
def startup_event():
    kafka_bus.connect()
    # Подписываемся на события Kafka
    kafka_bus.subscribe(
        callback=aggregator.process_event,
        group_id="analytics-service-group"
    )

class InternalEventPayload(BaseModel):
    event_data: Dict[str, Any]

@app.post("/internal/events", include_in_schema=False)
def receive_internal_event(payload: InternalEventPayload):
    aggregator.process_event(payload.event_data)
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "analytics-service"}

@app.get("/analytics/summary", summary="Получение сводной аналитики заказов")
def get_analytics_summary():
    return aggregator.get_summary()

@app.get("/analytics/events", summary="Получение последних обработанных событий")
def get_recent_events():
    return aggregator.get_recent_events()

@app.get("/", response_class=HTMLResponse, summary="Интерактивный дашборд аналитики")
def analytics_dashboard():
    summary = aggregator.get_summary()
    events = aggregator.get_recent_events(15)

    events_rows = ""
    for ev in events:
        ev_type = ev.get("eventType")
        color = "#238636" if ev_type == "OrderPaid" else ("#da3633" if ev_type == "OrderCancelled" else "#58a6ff")
        events_rows += f"""
        <tr>
            <td style="font-size: 11px; color: #8b949e;">{ev.get('timestamp')}</td>
            <td><span style="background: {color}; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold;">{ev_type}</span></td>
            <td style="font-family: monospace;">{ev.get('orderId')}</td>
            <td style="font-size: 11px;">v{ev.get('version')}</td>
            <td style="font-family: monospace; font-size: 11px; color: #c9d1d9;">{str(ev.get('payload'))}</td>
        </tr>
        """

    if not events_rows:
        events_rows = '<tr><td colspan="5" style="text-align: center; color: #8b949e; padding: 15px;">События пока не поступили. Создайте заказы на Order Service.</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Analytics Service — Сводка метрик заказов (Kafka CQRS)</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; line-height: 1.5; }}
        .container {{ max-width: 1150px; margin: 0 auto; }}
        .nav {{ display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }}
        .nav a {{ background: #21262d; border: 1px solid #30363d; padding: 8px 14px; border-radius: 6px; color: #58a6ff; text-decoration: none; font-size: 13px; font-weight: 500; }}
        .nav a:hover {{ background: #30363d; }}
        .nav a.active {{ background: #1f6feb; color: white; border-color: #58a6ff; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 22px; margin-bottom: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
        h1 {{ color: #58a6ff; margin: 0 0 10px 0; font-size: 24px; }}
        h2 {{ color: #c9d1d9; margin-top: 0; font-size: 18px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }}
        .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 15px; margin: 20px 0; }}
        .metric-box {{ background: #21262d; border: 1px solid #30363d; border-radius: 6px; padding: 16px; text-align: center; }}
        .metric-val {{ font-size: 26px; font-weight: bold; color: #58a6ff; }}
        .metric-label {{ font-size: 12px; color: #8b949e; margin-top: 4px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }}
        th, td {{ border: 1px solid #30363d; padding: 9px 12px; text-align: left; }}
        th {{ background: #21262d; color: #8b949e; }}
        tr:hover {{ background: #1c2128; }}
        .btn {{ cursor: pointer; background: #21262d; border: 1px solid #30363d; color: #c9d1d9; padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 500; }}
        .btn:hover {{ background: #30363d; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="http://localhost:8001/">📦 Order Service (Write & Read Side)</a>
            <a href="/" class="active">📊 Analytics Service Dashboard</a>
            <a href="http://localhost:8085/" target="_blank">🐙 Kafka UI (orders-events)</a>
            <a href="/analytics/summary" target="_blank">📄 JSON Summary</a>
            <span style="margin-left: auto; color: #8b949e; font-size: 12px; align-self: center;">ТПУ • Архитектура систем • ЛР №6</span>
        </div>

        <div class="card">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h1>Сервис оперативной аналитики заказов</h1>
                    <p style="color: #8b949e; margin: 0;">Агрегация показателей в реальном времени из потока событий Apache Kafka (топик: orders-events).</p>
                </div>
                <button class="btn" onclick="location.reload()">🔄 Обновить сводку</button>
            </div>

            <div class="metrics-grid">
                <div class="metric-box">
                    <div class="metric-val">{summary['total_orders']}</div>
                    <div class="metric-label">Всего создано заказов</div>
                </div>
                <div class="metric-box">
                    <div class="metric-val" style="color: #3fb950;">{summary['total_paid_revenue']:.2f} ₽</div>
                    <div class="metric-label">Выручка (оплаченные заказы)</div>
                </div>
                <div class="metric-box">
                    <div class="metric-val">{summary['average_order_amount']:.2f} ₽</div>
                    <div class="metric-label">Средний чек заказа</div>
                </div>
                <div class="metric-box">
                    <div class="metric-val" style="color: #d29922;">{summary['orders_by_status']['CREATED']} / <span style="color: #3fb950;">{summary['orders_by_status']['PAID']}</span> / <span style="color: #f85149;">{summary['orders_by_status']['CANCELLED']}</span></div>
                    <div class="metric-label">Статусы: Создан / Оплачен / Отменен</div>
                </div>
            </div>
        </div>

        <div class="card">
            <h2>Лента событий топика Kafka (orders-events)</h2>
            <table>
                <thead>
                    <tr>
                        <th style="width: 200px;">Время (UTC)</th>
                        <th style="width: 140px;">Тип события</th>
                        <th style="width: 220px;">Order ID</th>
                        <th style="width: 60px;">Версия</th>
                        <th>Полезная нагрузка (Payload)</th>
                    </tr>
                </thead>
                <tbody>
                    {events_rows}
                </tbody>
            </table>
        </div>
    </div>
</body>
</html>
"""
