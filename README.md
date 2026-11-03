# Лабораторная работа №6: CQRS и Event Sourcing для заказов

Проект распределенной микросервисной системы управления заказами, реализованной на основе архитектурных паттернов **CQRS** (Command Query Responsibility Segregation — разделение ответственности команд и запросов), **Event Sourcing** (хранение состояния как последовательности событий) и брокера сообщений **Apache Kafka**.

---

## Архитектура системы

В соответствии с принципами CQRS и Event Sourcing система разделена на сторону записи (Write Side) и сторону чтения (Read Side):

```
                        ┌────────────────────────────────────────────────────────┐
                        │                   БРАУЗЕР / КЛИЕНТ                     │
                        └──────────┬──────────────────────────────▲──────────────┘
                                   │                              │
                     POST /orders  │                 GET /orders  │
                POST /orders/{id}/pay                             │
              POST /orders/{id}/cancel                            │
                                   │                              │
                                   ▼                              │
                       ┌───────────────────────┐      ┌───────────┴──────────┐
                       │      WRITE SIDE       │      │      READ SIDE       │
                       │   (Command Handler)   │      │   (Query Handler)    │
                       └───────────┬───────────┘      └───────────▲──────────┘
                                   │                              │
                        Rehydrate  │                              │
                        & Append   ▼                              │ Обновление
                       ┌───────────────────────┐                  │ проекции
                       │      EVENT STORE      │                  │ (Projection)
                       │  (Single Source of    │                  │
                       │        Truth)         │                  │
                       └───────────┬───────────┘                  │
                                   │                              │
                                   │ Публикация событий           │
                                   ▼                              │
                       ┌──────────────────────────────────────────┴──────────┐
                       │               APACHE KAFKA (KRaft)                  │
                       │              Топик: orders-events                   │
                       └──────────────────────────┬──────────────────────────┘
                                                  │
                                                  │ Потоковая подписка
                                                  ▼
                                      ┌───────────────────────┐
                                      │   ANALYTICS SERVICE   │
                                      │ GET /analytics/summary│
                                      └───────────────────────┘
```

### Ключевые компоненты:
1. **Event Store (Единственный источник истины)**:
   - Таблица `event_store` с полями `eventId`, `orderId`, `eventType`, `payload`, `timestamp`, `version`.
   - **Состояние заказа не хранится как готовая строка в БД**. Оно динамически вычисляется воспроизведением событий (**Rehydration**).
   - Оптимистический контроль конкурентности (`UNIQUE(order_id, version)`).

2. **Write Side (Обработчик команд)**:
   - `POST /orders` — создание заказа (событие `OrderCreated`).
   - `POST /orders/{id}/pay` — оплата заказа (событие `OrderPaid`).
   - `POST /orders/{id}/cancel` — отмена заказа (событие `OrderCancelled`).
   - Перед обработкой каждой команды агрегат загружает события, проигрывает их (`apply`), проверяет инварианты (бизнес-правила) и сохраняет новое событие.

3. **Read Side (Денормализованная Read-модель)**:
   - Оптимизирована исключительно для быстрых запросов на чтение: `GET /orders` и `GET /orders/{id}`.
   - Читает **ТОЛЬКО** из структуры проекции, не нагружая Event Store.
   - Обновляется асинхронно при поступлении событий из Kafka.

4. **Apache Kafka (KRaft mode) + Kafka UI**:
   - Топик `orders-events` принимает все доменные события.
   - Kafka UI доступен на порту `8085` (`http://localhost:8085/`) для визуализации партиций и сообщений.

5. **Analytics Service (Сервис оперативной аналитики)**:
   - Порт `8002` (`http://localhost:8002/`).
   - Подписывается на Kafka-топик `orders-events` и агрегирует метрики: выручка, число заказов по статусам, средний чек.
   - Эндпоинт `GET /analytics/summary`.

---

## Структура проекта

```text
lab6-microservices/
├── common/
│   ├── events.py             # Спецификация событий (OrderCreated, OrderPaid, OrderCancelled)
│   ├── event_store.py        # Event Store (хранилище событий с контролем версий)
│   └── kafka_bus.py          # Клиент Apache Kafka (Producer/Consumer с авто-фолбэком)
├── order_service/
│   ├── aggregate.py          # Агрегат OrderAggregate (логика apply, rehydrate, бизнес-правила)
│   ├── projection.py         # Read-модель (декларативная проекция для GET)
│   ├── app.py                # FastAPI сервис: Write Side команды, Read Side запросы, Dashboard
│   └── Dockerfile
├── analytics_service/
│   ├── analytics.py          # Агрегатор оперативной статистики
│   ├── app.py                # FastAPI сервис аналитики и веб-дашборд
│   └── Dockerfile
├── docker-compose.yml        # Оркестрация: Kafka, Kafka UI, Order Service, Analytics Service
├── run_local.py              # Автономный скрипт локального запуска без Docker
├── test_cqrs_es.py           # Сквозной интеграционный автотест
└── requirements.txt          # Зависимости
```

---

## Быстрый запуск

### Вариант 1: Через Docker Compose (рекомендуется)
```bash
docker compose up --build
```
После запуска доступны:
- 📦 **Order Service Dashboard:** `http://localhost:8001/`
- 📊 **Analytics Dashboard:** `http://localhost:8002/`
- 🐙 **Kafka UI:** `http://localhost:8085/`
- 📄 **Swagger API:** `http://localhost:8001/docs`

### Вариант 2: Локальный запуск (без Docker)
1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Запустите комплекс сервисов:
   ```bash
   python run_local.py
   ```

---

## Тестирование и верификация

Запуск полного интеграционного теста:
```bash
python test_cqrs_es.py
```
Тест проверяет:
1. Создание заказа $\rightarrow$ добавление события `OrderCreated` в Event Store $\rightarrow$ асинхронное обновление Read-модели.
2. Оплата заказа $\rightarrow$ Rehydration агрегата $\rightarrow$ генерация `OrderPaid` (v2) $\rightarrow$ обновление статуса в `PAID`.
3. Валидация инвариантов: попытка повторной оплаты $\rightarrow$ отклонение с ошибкой HTTP 400 Bad Request, отсутствие лишних событий.
4. Отмена заказа $\rightarrow$ событие `OrderCancelled` (v3) $\rightarrow$ статус `CANCELLED`.
5. Потоковая агрегация в Analytics Service: пересчет выручки, количества заказов и среднего чека.

---

## Команда проекта

Студенты группы **8И42**, ТПУ:
* **Довидович Александр Владимирович** — разработка Order Service (Write Side команды, Rehydration, Event Store, Docker Compose).
* **Петрушенко Даниил Евгеньевич** — разработка Read-модели (OrderProjection), интеграция с Apache Kafka, Analytics Service и интеграционные автотесты.
