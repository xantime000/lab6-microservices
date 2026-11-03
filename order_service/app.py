import os
import sys
import uuid
import datetime
import requests
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, status, Query
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common.events import (
    DomainEvent,
    EVENT_ORDER_CREATED,
    EVENT_ORDER_PAID,
    EVENT_ORDER_CANCELLED
)
from common.event_store import EventStore, ConcurrencyException
from common.kafka_bus import KafkaEventBus
from order_service.aggregate import OrderAggregate, BusinessRuleViolationException
from order_service.projection import OrderProjection

SERVICE_PORT = int(os.getenv("PORT", "8001"))
EVENT_STORE_DB = os.getenv("EVENT_STORE_DB", "event_store.db")
ANALYTICS_SERVICE_URL = os.getenv("ANALYTICS_SERVICE_URL", "http://127.0.0.1:8002/internal/events")

app = FastAPI(
    title="Order Service (CQRS & Event Sourcing)",
    description="Сервис управления заказами на основе разделения ответственности команд и запросов (CQRS) и хранения событий (Event Sourcing)",
    version="1.0.0"
)

# 1. Event Store (Write Side — единственный источник истины)
event_store = EventStore(db_path=EVENT_STORE_DB)

# 2. Read-модель (Read Side — денормализованная проекция для быстрых GET-запросов)
order_projection = OrderProjection()

# 3. Шина сообщений Apache Kafka
kafka_bus = KafkaEventBus(service_name="OrderService")

@app.on_event("startup")
def startup_event():
    kafka_bus.connect()

    # Синхронизация проекции (Read Side): подписываемся на события топика
    def on_kafka_event(event_data: Dict[str, Any]):
        order_projection.handle_event(event_data)

    kafka_bus.subscribe(on_kafka_event, group_id="order-service-projection-group")

    # Инициализация read-модели существующими событиями из Event Store (replay при старте)
    all_events = event_store.get_all_events(limit=1000)
    for ev in reversed(all_events):
        order_projection.handle_event(ev.to_dict())

def publish_event(event: DomainEvent):
    """Публикация события в Kafka и отправка локальным подписчикам"""
    event_dict = event.to_dict()
    # 1. Публикуем в Kafka
    kafka_bus.publish(event_dict)
    # 2. Немедленно обновляем локальную Read-модель
    order_projection.handle_event(event_dict)
    # 3. Отправляем в сервис аналитики (fallback для работы без Kafka)
    try:
        requests.post(ANALYTICS_SERVICE_URL, json={"event_data": event_dict}, timeout=0.5)
    except Exception:
        pass

# =============================================================================
# WRITE SIDE (КОМАНДЫ CQRS)
# Состояние заказа НЕ хранится как сущность — только добавляются события!
# Перед каждой командой выполняется Rehydration (восстановление из событий).
# =============================================================================

class OrderItem(BaseModel):
    productId: str = Field(..., example="prod-101")
    quantity: int = Field(..., ge=1, example=2)
    price: float = Field(..., ge=0.0, example=750.0)

class CreateOrderCommand(BaseModel):
    userId: str = Field(..., example="user-alex-1")
    items: List[OrderItem]
    totalAmount: Optional[float] = Field(None, example=1500.0)

class PayOrderCommand(BaseModel):
    paymentMethod: Optional[str] = Field("CARD", example="CARD")

class CancelOrderCommand(BaseModel):
    reason: Optional[str] = Field("Отменено клиентом", example="Клиент передумал")

@app.post("/orders", status_code=status.HTTP_201_CREATED, summary="Команда: Создать заказ (POST)")
def create_order(cmd: CreateOrderCommand):
    order_id = str(uuid.uuid4())

    # Рассчитываем сумму заказа, если не передана явно
    calc_total = sum(i.quantity * i.price for i in cmd.items)
    total_amount = cmd.totalAmount if cmd.totalAmount is not None else calc_total

    # 1. Загружаем текущие события из Event Store (для нового заказа список пуст)
    events = event_store.get_events(order_id)

    # 2. Восстанавливаем состояние агрегата (Rehydration)
    aggregate = OrderAggregate.rehydrate(order_id, events)

    # 3. Проверяем бизнес-правила и генерируем событие OrderCreated
    try:
        new_event = aggregate.execute_create(
            user_id=cmd.userId,
            items=[i.model_dump() for i in cmd.items],
            total_amount=total_amount
        )
    except BusinessRuleViolationException as err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(err))

    # 4. Сохраняем событие в Event Store
    try:
        event_store.append_event(new_event)
    except ConcurrencyException as c_err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(c_err))

    # 5. Публикуем событие в Kafka
    publish_event(new_event)

    return {
        "orderId": order_id,
        "status": "CREATED",
        "version": new_event.version,
        "message": "Заказ успешно создан! Событие OrderCreated добавлено в Event Store и опубликовано в Kafka.",
        "eventId": new_event.eventId
    }

@app.post("/orders/{order_id}/pay", summary="Команда: Оплатить заказ (POST)")
def pay_order(order_id: str, cmd: PayOrderCommand = PayOrderCommand()):
    # 1. Загружаем историю событий заказа из Event Store
    events = event_store.get_events(order_id)
    if not events:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Заказ {order_id} не найден в Event Store")

    # 2. Восстанавливаем текущее состояние агрегата (Rehydration)
    aggregate = OrderAggregate.rehydrate(order_id, events)

    # 3. Проверяем бизнес-правила (нельзя оплатить уже оплаченный или отмененный заказ)
    try:
        new_event = aggregate.execute_pay(payment_method=cmd.paymentMethod or "CARD")
    except BusinessRuleViolationException as err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(err))

    # 4. Сохраняем новое событие OrderPaid в Event Store
    try:
        event_store.append_event(new_event)
    except ConcurrencyException as c_err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(c_err))

    # 5. Публикуем событие в Kafka
    publish_event(new_event)

    return {
        "orderId": order_id,
        "status": "PAID",
        "version": new_event.version,
        "message": f"Заказ {order_id} успешно оплачен! Событие OrderPaid сохранено в Event Store.",
        "eventId": new_event.eventId
    }

@app.post("/orders/{order_id}/cancel", summary="Команда: Отменить заказ (POST)")
def cancel_order(order_id: str, cmd: CancelOrderCommand = CancelOrderCommand()):
    # 1. Загружаем историю событий заказа из Event Store
    events = event_store.get_events(order_id)
    if not events:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Заказ {order_id} не найден в Event Store")

    # 2. Восстанавливаем текущее состояние агрегата (Rehydration)
    aggregate = OrderAggregate.rehydrate(order_id, events)

    # 3. Проверяем бизнес-правила (нельзя отменить уже отмененный заказ)
    try:
        new_event = aggregate.execute_cancel(reason=cmd.reason or "Отменено клиентом")
    except BusinessRuleViolationException as err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(err))

    # 4. Сохраняем событие OrderCancelled в Event Store
    try:
        event_store.append_event(new_event)
    except ConcurrencyException as c_err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(c_err))

    # 5. Публикуем событие в Kafka
    publish_event(new_event)

    return {
        "orderId": order_id,
        "status": "CANCELLED",
        "version": new_event.version,
        "message": f"Заказ {order_id} отменен! Событие OrderCancelled сохранено в Event Store.",
        "eventId": new_event.eventId
    }

# =============================================================================
# READ SIDE (ЗАПРОСЫ CQRS)
# Читают данные ТОЛЬКО из Read-модели (OrderProjection), а НЕ из Event Store!
# =============================================================================

@app.get("/orders", summary="Запрос: Получить все заказы (из Read-модели)")
def get_orders():
    return order_projection.get_all()

@app.get("/orders/{order_id}", summary="Запрос: Получить заказ по ID (из Read-модели)")
def get_order_by_id(order_id: str):
    order = order_projection.get_by_id(order_id)
    if not order:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Заказ {order_id} не найден в Read-модели")
    return order

# =============================================================================
# EVENT STORE ИНСПЕКТОР (ДЛЯ ДЕМОНСТРАЦИИ И АУДИТА НА ЗАЩИТЕ)
# =============================================================================

@app.get("/event-store/events", summary="Инспектор: Список всех сырых событий Event Store")
def get_raw_events(limit: int = 50):
    events = event_store.get_all_events(limit=limit)
    return [e.to_dict() for e in events]

@app.get("/event-store/orders/{order_id}", summary="Инспектор: Поток событий конкретного заказа")
def get_order_events(order_id: str):
    events = event_store.get_events(order_id)
    return [e.to_dict() for e in events]

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "order-service",
        "read_model_orders_count": order_projection.count(),
        "total_events_in_projection": order_projection.processed_events_count
    }

# =============================================================================
# ВЕБ-ДАШБОРД (ИНТЕРАКТИВНОЕ УПРАВЛЕНИЕ CQRS & EVENT SOURCING)
# =============================================================================

@app.get("/", response_class=HTMLResponse, summary="Интерактивный веб-дашборд")
def dashboard():
    read_orders = order_projection.get_all()
    raw_events = event_store.get_all_events(limit=25)

    # Таблица заказов из Read-модели
    orders_rows = ""
    for o in read_orders:
        st = o["status"]
        color = "#238636" if st == "PAID" else ("#da3633" if st == "CANCELLED" else "#d29922")
        items_str = ", ".join([f"{i['productId']} (x{i['quantity']})" for i in o.get("items", [])])
        
        actions = ""
        if st == "CREATED":
            actions = f"""
                <button class="btn btn-sm btn-success" onclick="payOrder('{o['orderId']}')">Оплатить</button>
                <button class="btn btn-sm btn-danger" onclick="cancelOrder('{o['orderId']}')">Отменить</button>
            """
        elif st == "PAID":
            actions = f"""
                <button class="btn btn-sm btn-secondary" onclick="tryInvalidPay('{o['orderId']}')">Повтор оплаты (сбой)</button>
                <button class="btn btn-sm btn-danger" onclick="cancelOrder('{o['orderId']}')">Отменить</button>
            """
        else: # CANCELLED
            actions = f"""
                <button class="btn btn-sm btn-secondary" onclick="tryInvalidCancel('{o['orderId']}')">Повтор отмены (сбой)</button>
            """

        orders_rows += f"""
        <tr>
            <td style="font-family: monospace; font-size: 12px;">{o['orderId']}</td>
            <td>{o['userId']}</td>
            <td>{items_str}</td>
            <td><strong>{o['totalAmount']:.2f} ₽</strong></td>
            <td><span style="background: {color}; color: white; padding: 3px 8px; border-radius: 12px; font-weight: bold; font-size: 11px;">{st}</span></td>
            <td>v{o['version']}</td>
            <td style="font-size: 11px; color: #8b949e;">{o['updatedAt']}</td>
            <td>{actions}</td>
        </tr>
        """

    if not orders_rows:
        orders_rows = '<tr><td colspan="8" style="text-align: center; color: #8b949e; padding: 15px;">В Read-модели пока нет заказов. Нажмите кнопку «Создать тестовый заказ» ниже.</td></tr>'

    # Таблица сырых событий из Event Store
    events_rows = ""
    for ev in raw_events:
        ev_type = ev.eventType
        ev_color = "#238636" if ev_type == "OrderPaid" else ("#da3633" if ev_type == "OrderCancelled" else "#58a6ff")
        events_rows += f"""
        <tr>
            <td style="font-size: 11px; color: #8b949e;">{ev.timestamp}</td>
            <td style="font-family: monospace; font-size: 11px;">{ev.eventId[:8]}...</td>
            <td style="font-family: monospace; font-size: 12px;">{ev.orderId}</td>
            <td><span style="background: {ev_color}; color: white; padding: 2px 7px; border-radius: 4px; font-size: 11px; font-weight: bold;">{ev_type}</span></td>
            <td><strong>v{ev.version}</strong></td>
            <td style="font-family: monospace; font-size: 11px; color: #8b949e;">{str(ev.payload)}</td>
        </tr>
        """

    if not events_rows:
        events_rows = '<tr><td colspan="6" style="text-align: center; color: #8b949e; padding: 15px;">Event Store пуст. Создайте заказ для генерации первого события OrderCreated.</td></tr>'

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8">
    <title>Order Service — CQRS & Event Sourcing Dashboard</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0d1117; color: #c9d1d9; padding: 20px; line-height: 1.5; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .nav {{ display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }}
        .nav a {{ background: #21262d; border: 1px solid #30363d; padding: 8px 14px; border-radius: 6px; color: #58a6ff; text-decoration: none; font-size: 13px; font-weight: 500; }}
        .nav a:hover {{ background: #30363d; }}
        .nav a.active {{ background: #1f6feb; color: white; border-color: #58a6ff; }}
        .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 22px; margin-bottom: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.3); }}
        h1 {{ color: #58a6ff; margin: 0 0 10px 0; font-size: 24px; }}
        h2 {{ color: #c9d1d9; margin-top: 0; font-size: 18px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }}
        th, td {{ border: 1px solid #30363d; padding: 8px 10px; text-align: left; }}
        th {{ background: #21262d; color: #8b949e; }}
        tr:hover {{ background: #1c2128; }}
        .btn {{ cursor: pointer; padding: 8px 16px; border-radius: 6px; border: none; font-size: 13px; font-weight: bold; transition: 0.2s; }}
        .btn-sm {{ padding: 3px 8px; font-size: 11px; }}
        .btn-primary {{ background: #1f6feb; color: white; }}
        .btn-primary:hover {{ background: #388bfd; }}
        .btn-success {{ background: #238636; color: white; }}
        .btn-success:hover {{ background: #2ea043; }}
        .btn-danger {{ background: #da3633; color: white; }}
        .btn-danger:hover {{ background: #f85149; }}
        .btn-secondary {{ background: #30363d; color: #c9d1d9; border: 1px solid #8b949e; }}
        .btn-secondary:hover {{ background: #484f58; }}
        .btn-group {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }}
        .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        .alert {{ padding: 12px 16px; border-radius: 6px; margin-bottom: 15px; font-size: 13px; }}
        .alert-info {{ background: #161b22; border: 1px solid #1f6feb; color: #58a6ff; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="nav">
            <a href="/" class="active">📦 Order Service (Write & Read Side)</a>
            <a href="http://localhost:8002/">📊 Analytics Service Dashboard</a>
            <a href="http://localhost:8085/" target="_blank">🐙 Kafka UI (orders-events)</a>
            <a href="/docs" target="_blank">📄 Swagger / OpenAPI</a>
            <span style="margin-left: auto; color: #8b949e; font-size: 12px; align-self: center;">ТПУ • Архитектура систем • ЛР №6</span>
        </div>

        <div class="card">
            <h1>CQRS & Event Sourcing: Управление заказами</h1>
            <p style="color: #8b949e; margin: 0 0 15px 0;">
                Демонстрация разделения записи команд (Write Side) и чтения (Read Side), восстановления состояния (Rehydration)
                и потоковой трансляции доменных событий через Apache Kafka.
            </p>

            <div class="alert alert-info">
                💡 <strong>Архитектурная особенность:</strong> На стороне записи (Write Side) таблица состояния заказа ОТСУТСТВУЕТ.
                Единственный источник истины — цепочка событий в Event Store. Перед каждой операцией агрегат восстанавливается
                (Rehydration) из истории событий, валидирует бизнес-правила и сохраняет новое событие. Read-модель обновляется асинхронно!
            </div>

            <h3 style="margin-bottom: 8px;">Команды управления (Write Side):</h3>
            <div class="btn-group">
                <button class="btn btn-primary" onclick="createSampleOrder('Ноутбук Lenovo', 75000)">+ 1. Создать заказ (75 000 ₽)</button>
                <button class="btn btn-primary" onclick="createSampleOrder('Смартфон Pixel', 48000)">+ 2. Создать заказ (48 000 ₽)</button>
                <button class="btn btn-secondary" onclick="location.reload()">🔄 Обновить страницу</button>
            </div>
        </div>

        <div class="card">
            <h2>Read Side: Проекция заказов (GET /orders)</h2>
            <p style="color: #8b949e; font-size: 12px; margin-top: -5px;">Данные считываются ТОЛЬКО из денормализованной Read-модели, оптимизированной под быстрые выборки.</p>
            <table>
                <thead>
                    <tr>
                        <th>Order ID</th>
                        <th>Клиент</th>
                        <th>Товары</th>
                        <th>Сумма</th>
                        <th>Статус</th>
                        <th>Версия</th>
                        <th>Обновлено</th>
                        <th>Команды (Write Side)</th>
                    </tr>
                </thead>
                <tbody>
                    {orders_rows}
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>Write Side: Event Store (Сырые события в порядке версий)</h2>
            <p style="color: #8b949e; font-size: 12px; margin-top: -5px;">Неизменяемый журнал (Append-Only Log) всех доменных фактов: OrderCreated, OrderPaid, OrderCancelled.</p>
            <table>
                <thead>
                    <tr>
                        <th style="width: 180px;">Время (UTC)</th>
                        <th style="width: 100px;">Event ID</th>
                        <th style="width: 250px;">Order ID</th>
                        <th style="width: 130px;">Тип события</th>
                        <th style="width: 70px;">Версия</th>
                        <th>Payload события</th>
                    </tr>
                </thead>
                <tbody>
                    {events_rows}
                </tbody>
            </table>
        </div>
    </div>

    <script>
        function createSampleOrder(name, price) {{
            const payload = {{
                userId: 'client-' + Math.floor(Math.random() * 100),
                items: [{{ productId: name, quantity: 1, price: price }}],
                totalAmount: price
            }};
            fetch('/orders', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(payload)
            }})
            .then(res => res.json())
            .then(data => {{
                location.reload();
            }})
            .catch(err => alert('Ошибка создания: ' + err));
        }}

        function payOrder(orderId) {{
            fetch(`/orders/${{orderId}}/pay`, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ paymentMethod: 'CARD' }}) }})
            .then(res => res.json())
            .then(data => location.reload())
            .catch(err => alert('Ошибка оплаты: ' + err));
        }}

        function cancelOrder(orderId) {{
            const reason = prompt('Укажите причину отмены:', 'Клиент передумал');
            if (!reason) return;
            fetch(`/orders/${{orderId}}/cancel`, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ reason: reason }}) }})
            .then(res => res.json())
            .then(data => location.reload())
            .catch(err => alert('Ошибка отмены: ' + err));
        }}

        function tryInvalidPay(orderId) {{
            fetch(`/orders/${{orderId}}/pay`, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ paymentMethod: 'CARD' }}) }})
            .then(res => res.json())
            .then(data => {{
                alert('Проверка бизнес-правила успешно сработала!\\nОтвет сервера: ' + JSON.stringify(data));
            }});
        }}

        function tryInvalidCancel(orderId) {{
            fetch(`/orders/${{orderId}}/cancel`, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ reason: 'test' }}) }})
            .then(res => res.json())
            .then(data => {{
                alert('Проверка бизнес-правила успешно сработала!\\nОтвет сервера: ' + JSON.stringify(data));
            }});
        }}
    </script>
</body>
</html>
"""
