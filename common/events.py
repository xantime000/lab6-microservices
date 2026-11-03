import uuid
import datetime
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class DomainEvent(BaseModel):
    """
    Базовая модель доменного события в архитектуре Event Sourcing.
    Каждое событие неизменяемо (immutable) и фиксирует факт, произошедший в системе.
    """
    eventId: str = Field(default_factory=lambda: str(uuid.uuid4()))
    orderId: str
    eventType: str
    payload: Dict[str, Any]
    timestamp: str = Field(default_factory=lambda: datetime.datetime.utcnow().isoformat() + "Z")
    version: int

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump()

# Константы типов событий
EVENT_ORDER_CREATED = "OrderCreated"
EVENT_ORDER_PAID = "OrderPaid"
EVENT_ORDER_CANCELLED = "OrderCancelled"
