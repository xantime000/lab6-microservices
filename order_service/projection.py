import threading
from typing import Dict, Any, List, Optional
from common.events import (
    EVENT_ORDER_CREATED,
    EVENT_ORDER_PAID,
    EVENT_ORDER_CANCELLED
)

class OrderProjection:
    """
    Read-модель (CQRS Projection).
    Денормализованное хранилище, оптимизированное исключительно для быстрых запросов на чтение (GET).
    Обновляется асинхронно при получении доменных событий.
    Эндпоинты чтения (GET /orders, GET /orders/{id}) запрашивают данные ТОЛЬКО отсюда,
    никогда не обращаясь к Event Store напрямую.
    """
    def __init__(self):
        self._lock = threading.Lock()
        # Хранилище проекций заказов: order_id -> dict
        self._orders: Dict[str, Dict[str, Any]] = {}
        self.processed_events_count = 0

    def handle_event(self, event_data: Dict[str, Any]):
        """
        Проекционный обработчик (Event Handler).
        Асинхронно мутирует Read-модель на основе поступившего события из Kafka.
        """
        with self._lock:
            self.processed_events_count += 1
            event_type = event_data.get("eventType")
            order_id = event_data.get("orderId")
            payload = event_data.get("payload", {})
            timestamp = event_data.get("timestamp")
            version = event_data.get("version", 1)

            if event_type == EVENT_ORDER_CREATED:
                self._orders[order_id] = {
                    "orderId": order_id,
                    "userId": payload.get("userId"),
                    "items": payload.get("items", []),
                    "totalAmount": payload.get("totalAmount", 0.0),
                    "status": "CREATED",
                    "version": version,
                    "createdAt": timestamp,
                    "updatedAt": timestamp,
                    "paymentInfo": None,
                    "cancellationReason": None
                }

            elif event_type == EVENT_ORDER_PAID:
                if order_id in self._orders:
                    self._orders[order_id]["status"] = "PAID"
                    self._orders[order_id]["version"] = version
                    self._orders[order_id]["updatedAt"] = timestamp
                    self._orders[order_id]["paymentInfo"] = {
                        "paymentMethod": payload.get("paymentMethod", "CARD"),
                        "paidAmount": payload.get("amount"),
                        "paidAt": timestamp
                    }

            elif event_type == EVENT_ORDER_CANCELLED:
                if order_id in self._orders:
                    self._orders[order_id]["status"] = "CANCELLED"
                    self._orders[order_id]["version"] = version
                    self._orders[order_id]["updatedAt"] = timestamp
                    self._orders[order_id]["cancellationReason"] = payload.get("reason")

    def get_by_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Чтение одного заказа из read-модели"""
        with self._lock:
            order = self._orders.get(order_id)
            return dict(order) if order else None

    def get_all(self) -> List[Dict[str, Any]]:
        """Чтение всех заказов из read-модели"""
        with self._lock:
            return [dict(o) for o in reversed(list(self._orders.values()))]

    def count(self) -> int:
        with self._lock:
            return len(self._orders)
