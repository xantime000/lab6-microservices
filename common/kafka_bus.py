import os
import json
import time
import socket
import logging
import threading
from typing import Callable, List, Dict, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("KafkaBus")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders-events")

def is_socket_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            return s.connect_ex((host, port)) == 0
    except Exception:
        return False

class KafkaEventBus:
    """
    Шина событий для публикации и потребления сообщений Apache Kafka.
    1. При наличии запущенной Kafka (через Docker Compose): использует kafka-python
       (KafkaProducer и KafkaConsumer) с топиком orders-events.
    2. При локальном тестировании без Kafka: активирует встроенный диспетчер,
       гарантируя работу тестов и скрипта run_local.py.
    """
    def __init__(self, service_name: str, topic: str = KAFKA_TOPIC):
        self.service_name = service_name
        self.topic = topic
        self.logger = logging.getLogger(service_name)
        self._producer = None
        self._is_kafka_connected = False
        self._subscribers: List[Callable[[Dict[str, Any]], None]] = []
        self._lock = threading.Lock()

    def connect(self, max_retries: int = 15, retry_interval: float = 2.0) -> bool:
        """Подключение Producer к брокеру Kafka с повторными попытками"""
        try:
            from kafka import KafkaProducer
        except ImportError:
            self.logger.warning("Пакет kafka-python не установлен. Используется локальная шина.")
            return False

        # Если локальный запуск на localhost и порт закрыт — не ждем долго
        if KAFKA_BOOTSTRAP_SERVERS.startswith("localhost") or KAFKA_BOOTSTRAP_SERVERS.startswith("127.0.0.1"):
            if not is_socket_open("127.0.0.1", 9092, timeout=0.2):
                self.logger.info("Локальная Kafka на localhost:9092 не запущена. Используется встроенная шина.")
                return False

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Подключение к Kafka ({KAFKA_BOOTSTRAP_SERVERS}), попытка {attempt}/{max_retries}...")
                self._producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    acks="all",
                    retries=3,
                    request_timeout_ms=5000
                )
                self._is_kafka_connected = True
                self.logger.info(f"Producer успешно подключен к Apache Kafka (топик: {self.topic})!")
                self._ensure_topic_exists()
                return True
            except Exception as e:
                self.logger.warning(f"Ожидание запуска брокера Kafka ({attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(retry_interval)
                else:
                    self.logger.error("Превышено время ожидания Kafka. Включаю автономную шину событий.")
                    return False
        return False

    def _ensure_topic_exists(self):
        """Автоматическое создание топика при подключении, если он еще не существует"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=4000
            )
            existing = admin.list_topics()
            if self.topic not in existing:
                self.logger.info(f"Создание топика '{self.topic}' (партиций: 1, репликация: 1)...")
                admin.create_topics([NewTopic(name=self.topic, num_partitions=1, replication_factor=1)])
                self.logger.info(f"Топик '{self.topic}' успешно создан!")
            admin.close()
        except Exception as e:
            self.logger.debug(f"Автосоздание топика: {e}")

    def publish(self, event_data: Dict[str, Any]):
        """Публикация доменного события в топик Kafka"""
        event_type = event_data.get("eventType", "Unknown")
        order_id = event_data.get("orderId", "Unknown")
        self.logger.info(f"--> [KAFKA PUBLISH] Топик '{self.topic}' | Событие '{event_type}' для заказа '{order_id}'")

        # Если при старте Kafka еще не была готова — пытаемся подключиться
        if not self._is_kafka_connected:
            self.connect(max_retries=2, retry_interval=1.0)

        if self._is_kafka_connected and self._producer:
            try:
                future = self._producer.send(
                    self.topic,
                    key=order_id.encode("utf-8"),
                    value=event_data
                )
                self._producer.flush(timeout=3)
                self.logger.info(f"Событие {event_type} успешно зафиксировано в Kafka.")
            except Exception as e:
                self.logger.error(f"Ошибка публикации в Kafka: {e}")

        # Уведомляем локальных подписчиков (для сервиса аналитики и проекций)
        self._notify_subscribers(event_data)

    def subscribe(self, callback: Callable[[Dict[str, Any]], None], group_id: Optional[str] = None):
        """Подписка на поток событий топика Kafka"""
        with self._lock:
            self._subscribers.append(callback)

        thread = threading.Thread(
            target=self._start_consumer_thread,
            args=(callback, group_id or f"{self.service_name}-group"),
            daemon=True
        )
        thread.start()

    def _start_consumer_thread(self, callback: Callable[[Dict[str, Any]], None], group_id: str):
        while True:
            try:
                from kafka import KafkaConsumer
                self.logger.info(f"Запуск Consumer для группы '{group_id}' на топике '{self.topic}' ({KAFKA_BOOTSTRAP_SERVERS})...")
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000
                )
                self.logger.info(f"Consumer для '{group_id}' успешно слушает топик '{self.topic}'!")

                while True:
                    for msg in consumer:
                        event_data = msg.value
                        self.logger.info(
                            f"<-- [KAFKA CONSUME] Топик '{msg.topic}' [Partition {msg.partition}, Offset {msg.offset}]: "
                            f"{event_data.get('eventType')}"
                        )
                        callback(event_data)
                    time.sleep(0.2)
            except Exception as e:
                time.sleep(2.0)

    def _notify_subscribers(self, event_data: Dict[str, Any]):
        with self._lock:
            subscribers_copy = list(self._subscribers)
        for cb in subscribers_copy:
            try:
                cb(event_data)
            except Exception as e:
                self.logger.error(f"Ошибка в обработчике события: {e}")
