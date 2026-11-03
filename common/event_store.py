import os
import json
import sqlite3
import threading
from typing import List, Dict, Any, Optional
from common.events import DomainEvent

class ConcurrencyException(Exception):
    """Исключение при конфликте версий (Optimistic Locking)"""
    pass

class EventStore:
    """
    Хранилище событий (Event Store) — единственный источник истины (Single Source of Truth).
    Хранит только последовательность неизменяемых доменных событий.
    Состояние заказа не хранится как готовая сущность, а восстанавливается из цепочки событий.
    """
    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # Таблица событий с уникальным составным индексом (order_id + version)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS event_store (
                        event_id TEXT PRIMARY KEY,
                        order_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        UNIQUE(order_id, version)
                    )
                """)
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_id ON event_store(order_id)")
                conn.commit()
            finally:
                conn.close()

    def append_event(self, event: DomainEvent):
        """
        Добавление нового события в хранилище с контролем оптимистической блокировки (версионности).
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # Проверяем текущую максимальную версию заказа
                cursor.execute("SELECT MAX(version) as max_v FROM event_store WHERE order_id = ?", (event.orderId,))
                row = cursor.fetchone()
                current_version = row["max_v"] if row["max_v"] is not None else 0

                # Проверка контроля конкурентности
                if event.version != current_version + 1:
                    raise ConcurrencyException(
                        f"Конфликт версий для заказа {event.orderId}! "
                        f"Текущая версия в Event Store: {current_version}, ожидалась: {event.version - 1}"
                    )

                cursor.execute("""
                    INSERT INTO event_store (event_id, order_id, event_type, payload, timestamp, version)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    event.eventId,
                    event.orderId,
                    event.eventType,
                    json.dumps(event.payload, ensure_ascii=False),
                    event.timestamp,
                    event.version
                ))
                conn.commit()
            except sqlite3.IntegrityError as err:
                conn.rollback()
                raise ConcurrencyException(f"Событие с такой версией уже существует: {err}")
            finally:
                conn.close()

    def get_events(self, order_id: str) -> List[DomainEvent]:
        """
        Загрузка всех событий конкретного заказа по возрастанию версии (для Rehydration).
        """
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT event_id, order_id, event_type, payload, timestamp, version
                    FROM event_store
                    WHERE order_id = ?
                    ORDER BY version ASC
                """, (order_id,))
                rows = cursor.fetchall()
                events: List[DomainEvent] = []
                for r in rows:
                    events.append(DomainEvent(
                        eventId=r["event_id"],
                        orderId=r["order_id"],
                        eventType=r["event_type"],
                        payload=json.loads(r["payload"]),
                        timestamp=r["timestamp"],
                        version=r["version"]
                    ))
                return events
            finally:
                conn.close()

    def get_all_events(self, limit: int = 100) -> List[DomainEvent]:
        """Получение списка последних событий всех заказов (для инспектора и аудита)"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT event_id, order_id, event_type, payload, timestamp, version
                    FROM event_store
                    ORDER BY rowid DESC
                    LIMIT ?
                """, (limit,))
                rows = cursor.fetchall()
                events: List[DomainEvent] = []
                for r in rows:
                    events.append(DomainEvent(
                        eventId=r["event_id"],
                        orderId=r["order_id"],
                        eventType=r["event_type"],
                        payload=json.loads(r["payload"]),
                        timestamp=r["timestamp"],
                        version=r["version"]
                    ))
                return events
            finally:
                conn.close()

    def get_latest_version(self, order_id: str) -> int:
        """Получение текущей версии заказа"""
        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(version) as max_v FROM event_store WHERE order_id = ?", (order_id,))
                row = cursor.fetchone()
                return row["max_v"] if row["max_v"] is not None else 0
            finally:
                conn.close()
