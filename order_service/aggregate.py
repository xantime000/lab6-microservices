from typing import List, Dict, Any, Optional
from common.events import (
    DomainEvent,
    EVENT_ORDER_CREATED,
    EVENT_ORDER_PAID,
    EVENT_ORDER_CANCELLED
)

class BusinessRuleViolationException(Exception):
    """Исключение при нарушении инвариантов бизнес-логики агрегата"""
    pass

class OrderAggregate:
    """
    Агрегат заказа (Domain Aggregate Root).
    В архитектуре Event Sourcing агрегат НЕ хранит свое состояние в БД напрямую.
    Состояние восстанавливается вызовом метода rehydrate(), проигрывающего поток событий.
    Агрегат проверяет бизнес-правила и порождает новые события.
    """
    def __init__(self, order_id: str):
        self.order_id = order_id
        self.user_id: Optional[str] = None
        self.items: List[Dict[str, Any]] = []
        self.total_amount: float = 0.0
        self.status: Optional[str] = None  # None -> CREATED -> PAID -> CANCELLED
        self.created_at: Optional[str] = None
        self.paid_at: Optional[str] = None
        self.cancelled_at: Optional[str] = None
        self.cancellation_reason: Optional[str] = None
        self.version: int = 0

    @classmethod
    def rehydrate(cls, order_id: str, events: List[DomainEvent]) -> 'OrderAggregate':
        """
        Восстановление состояния агрегата (Rehydration) из сохраненного массива событий.
        Вызывается перед выполнением любой входящей команды.
        """
        aggregate = cls(order_id)
        for event in events:
            aggregate.apply(event)
        return aggregate

    def apply(self, event: DomainEvent):
        """
        Применение исторического события к агрегату (мутация внутреннего состояния).
        Здесь НЕТ валидации бизнес-правил, так как событие уже произошло в прошлом.
        """
        self.version = event.version
        if event.eventType == EVENT_ORDER_CREATED:
            self.user_id = event.payload.get("userId")
            self.items = event.payload.get("items", [])
            self.total_amount = float(event.payload.get("totalAmount", 0.0))
            self.status = "CREATED"
            self.created_at = event.timestamp
        elif event.eventType == EVENT_ORDER_PAID:
            self.status = "PAID"
            self.paid_at = event.timestamp
        elif event.eventType == EVENT_ORDER_CANCELLED:
            self.status = "CANCELLED"
            self.cancelled_at = event.timestamp
            self.cancellation_reason = event.payload.get("reason", "Не указана")

    # =========================================================================
    # БИЗНЕС-КОМАНДЫ (ПРОВЕРКА ПРАВИЛ И ГЕНЕРАЦИЯ НОВЫХ СОБЫТИЙ)
    # =========================================================================

    def execute_create(self, user_id: str, items: List[Dict[str, Any]], total_amount: float) -> DomainEvent:
        """
        Команда создания нового заказа.
        Бизнес-правило: нельзя создать уже существующий заказ, сумма > 0, список товаров не пуст.
        """
        if self.status is not None:
            raise BusinessRuleViolationException(f"Заказ {self.order_id} уже существует в статусе {self.status}")
        if total_amount <= 0:
            raise BusinessRuleViolationException(f"Некорректная сумма заказа: {total_amount:.2f} руб. (должна быть > 0)")
        if not items:
            raise BusinessRuleViolationException("Заказ должен содержать минимум один товар")

        return DomainEvent(
            orderId=self.order_id,
            eventType=EVENT_ORDER_CREATED,
            payload={
                "userId": user_id,
                "items": items,
                "totalAmount": total_amount
            },
            version=self.version + 1
        )

    def execute_pay(self, payment_method: str = "CARD") -> DomainEvent:
        """
        Команда оплаты заказа.
        Бизнес-правила:
        - Заказ должен существовать.
        - Нельзя оплатить уже оплаченный заказ.
        - Нельзя оплатить отмененный заказ.
        """
        if self.status is None:
            raise BusinessRuleViolationException(f"Заказ {self.order_id} не найден!")
        if self.status == "PAID":
            raise BusinessRuleViolationException(f"Бизнес-ошибка: Заказ {self.order_id} уже был оплачен ранее!")
        if self.status == "CANCELLED":
            raise BusinessRuleViolationException(f"Бизнес-ошибка: Невозможно оплатить отмененный заказ {self.order_id}!")

        return DomainEvent(
            orderId=self.order_id,
            eventType=EVENT_ORDER_PAID,
            payload={
                "paymentMethod": payment_method,
                "amount": self.total_amount
            },
            version=self.version + 1
        )

    def execute_cancel(self, reason: str = "Отменено клиентом") -> DomainEvent:
        """
        Команда отмены заказа.
        Бизнес-правила:
        - Заказ должен существовать.
        - Нельзя отменить уже отмененный заказ.
        """
        if self.status is None:
            raise BusinessRuleViolationException(f"Заказ {self.order_id} не найден!")
        if self.status == "CANCELLED":
            raise BusinessRuleViolationException(f"Бизнес-ошибка: Заказ {self.order_id} уже отменен!")

        return DomainEvent(
            orderId=self.order_id,
            eventType=EVENT_ORDER_CANCELLED,
            payload={
                "reason": reason
            },
            version=self.version + 1
        )
