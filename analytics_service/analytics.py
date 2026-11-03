import threading
from typing import Dict, Any, List
from common.events import (
    EVENT_ORDER_CREATED,
    EVENT_ORDER_PAID,
    EVENT_ORDER_CANCELLED
)

class AnalyticsAggregator:
    """
    Агрегатор аналитических данных (Analytics Service).
    Слушает события заказов из Kafka (orders-events) и строит оперативную статистику.
    """
    def __init__(self):
        self._lock = threading.Lock()
        self.orders: Dict[str, Dict[str, Any]] = {}  # orderId -> {amount, status, created_at, paid_at}
        self.events_history: List[Dict[str, Any]] = []

    def process_event(self, event_data: Dict[str, Any]):
        """Обработка входящего события из Kafka"""
        with self._lock:
            self.events_history.append(event_data)
            event_type = event_data.get("eventType")
            order_id = event_data.get("orderId")
            payload = event_data.get("payload", {})
            timestamp = event_data.get("timestamp")

            if event_type == EVENT_ORDER_CREATED:
                self.orders[order_id] = {
                    "orderId": order_id,
                    "amount": float(payload.get("totalAmount", 0.0)),
                    "status": "CREATED",
                    "createdAt": timestamp
                }
            elif event_type == EVENT_ORDER_PAID:
                if order_id in self.orders:
                    self.orders[order_id]["status"] = "PAID"
                    self.orders[order_id]["paidAt"] = timestamp
            elif event_type == EVENT_ORDER_CANCELLED:
                if order_id in self.orders:
                    self.orders[order_id]["status"] = "CANCELLED"
                    self.orders[order_id]["cancelledAt"] = timestamp

    def get_summary(self) -> Dict[str, Any]:
        """Расчет агрегированной аналитической сводки (GET /analytics/summary)"""
        with self._lock:
            total_orders = len(self.orders)
            paid_orders = [o for o in self.orders.values() if o["status"] == "PAID"]
            created_orders = [o for o in self.orders.values() if o["status"] == "CREATED"]
            cancelled_orders = [o for o in self.orders.values() if o["status"] == "CANCELLED"]

            total_paid_revenue = sum(o["amount"] for o in paid_orders)
            all_amounts = [o["amount"] for o in self.orders.values()]
            average_order_amount = round(sum(all_amounts) / total_orders, 2) if total_orders > 0 else 0.0

            return {
                "total_orders": total_orders,
                "total_paid_revenue": round(total_paid_revenue, 2),
                "average_order_amount": average_order_amount,
                "orders_by_status": {
                    "CREATED": len(created_orders),
                    "PAID": len(paid_orders),
                    "CANCELLED": len(cancelled_orders)
                },
                "total_events_processed": len(self.events_history)
            }

    def get_recent_events(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._lock:
            return list(reversed(self.events_history[-limit:]))
