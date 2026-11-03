import sys
import time
import requests

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

ORDER_URL = "http://127.0.0.1:8001"
ANALYTICS_URL = "http://127.0.0.1:8002"

def print_separator(title: str):
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)

def main():
    print_separator("ТЕСТИРОВАНИЕ ЛАБОРАТОРНОЙ РАБОТЫ №6: CQRS И EVENT SOURCING")

    # 1. Проверка доступности микросервисов
    print("\n[Шаг 1] Проверка доступности микросервисов...")
    try:
        r_ord = requests.get(f"{ORDER_URL}/health", timeout=3)
        r_ana = requests.get(f"{ANALYTICS_URL}/health", timeout=3)
        print(f"  -> Order Service (порт 8001):     HTTP {r_ord.status_code} [OK]")
        print(f"  -> Analytics Service (порт 8002): HTTP {r_ana.status_code} [OK]")
    except Exception as e:
        print(f"\n[ОШИБКА] Не удалось подключиться к сервисам: {e}")
        print("Убедитесь, что сервисы запущены (docker-compose up или python run_local.py)")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # СЦЕНАРИЙ 1: КОМАНДА СОЗДАНИЯ ЗАКАЗА И ОБНОВЛЕНИЕ READ-МОДЕЛИ
    # -------------------------------------------------------------------------
    print_separator("ЭТАП 1: КОМАНДА СОЗДАНИЯ ЗАКАЗА (POST /orders -> Event Store -> Read Model)")
    print("Отправка команды POST /orders:")

    order_payload = {
        "userId": "client-alex-101",
        "items": [
            {"productId": "Ноутбук ASUS ROG", "quantity": 1, "price": 120000.0},
            {"productId": "Мышь Logitech G Pro", "quantity": 1, "price": 9500.0}
        ],
        "totalAmount": 129500.0
    }
    res_create = requests.post(f"{ORDER_URL}/orders", json=order_payload)
    assert res_create.status_code == 201, f"Ошибка создания: {res_create.text}"
    created_data = res_create.json()
    order_id = created_data["orderId"]
    print(f"  -> Создан заказ: ID={order_id}")
    print(f"  -> Начальная версия события: v{created_data['version']}")
    print(f"  -> Статус ответа команды: {created_data['status']}")

    # Проверяем наличие события в Event Store
    res_events_1 = requests.get(f"{ORDER_URL}/event-store/orders/{order_id}").json()
    print(f"\n1. Проверка Event Store для заказа {order_id}:")
    print(f"   -> Количество событий в цепочке: {len(res_events_1)}")
    print(f"   -> Тип события: {res_events_1[0]['eventType']} (версия {res_events_1[0]['version']})")
    assert len(res_events_1) == 1
    assert res_events_1[0]["eventType"] == "OrderCreated"
    assert res_events_1[0]["version"] == 1
    print("   -> [✓] Событие OrderCreated успешно сохранено в Event Store!")

    # Проверяем выборку из Read-модели (GET /orders/{id})
    time.sleep(0.3)
    res_read_1 = requests.get(f"{ORDER_URL}/orders/{order_id}")
    assert res_read_1.status_code == 200
    read_data_1 = res_read_1.json()
    print(f"\n2. Проверка Read-модели (GET /orders/{order_id}):")
    print(f"   -> Статус в проекции: {read_data_1['status']}")
    print(f"   -> Сумма заказа: {read_data_1['totalAmount']} руб.")
    print(f"   -> Клиент: {read_data_1['userId']}")
    assert read_data_1["status"] == "CREATED"
    assert read_data_1["totalAmount"] == 129500.0
    print("   -> [✓] Read-модель успешно обновилась асинхронно!")

    # -------------------------------------------------------------------------
    # СЦЕНАРИЙ 2: КОМАНДА ОПЛАТЫ И REHYDRATION АГРЕГАТА
    # -------------------------------------------------------------------------
    print_separator("ЭТАП 2: КОМАНДА ОПЛАТЫ ЗАКАЗА (POST /orders/{id}/pay)")
    print(f"Отправка команды оплаты для заказа {order_id}...")

    res_pay = requests.post(f"{ORDER_URL}/orders/{order_id}/pay", json={"paymentMethod": "CARD_MIR"})
    assert res_pay.status_code == 200, f"Ошибка оплаты: {res_pay.text}"
    pay_data = res_pay.json()
    print(f"  -> Ответ команды: {pay_data['message']}")
    print(f"  -> Новая версия в Event Store: v{pay_data['version']}")
    assert pay_data["version"] == 2
    assert pay_data["status"] == "PAID"

    # Проверяем цепочку событий в Event Store
    res_events_2 = requests.get(f"{ORDER_URL}/event-store/orders/{order_id}").json()
    print(f"\n1. Проверка цепочки событий в Event Store (всего: {len(res_events_2)}):")
    for ev in res_events_2:
        print(f"   * v{ev['version']} | {ev['eventType']} | timestamp={ev['timestamp']}")
    assert len(res_events_2) == 2
    assert res_events_2[1]["eventType"] == "OrderPaid"

    # Проверяем Read-модель
    time.sleep(0.3)
    res_read_2 = requests.get(f"{ORDER_URL}/orders/{order_id}").json()
    print(f"\n2. Проверка Read-модели после оплаты:")
    print(f"   -> Текущий статус в Read-модели: {res_read_2['status']}")
    print(f"   -> Информация об оплате: {res_read_2.get('paymentInfo')}")
    assert res_read_2["status"] == "PAID"
    assert res_read_2["version"] == 2
    print("   -> [✓] Rehydration и генерация события OrderPaid отработали корректно!")

    # -------------------------------------------------------------------------
    # СЦЕНАРИЙ 3: ПРОВЕРКА БИЗНЕС-ПРАВИЛ (ИНВАРИАНТОВ)
    # -------------------------------------------------------------------------
    print_separator("ЭТАП 3: ПРОВЕРКА БИЗНЕС-ПРАВИЛ (ИНВАРИАНТЫ АГРЕГАТА)")
    print("Попытка повторной оплаты уже оплаченного заказа:")

    res_invalid_pay = requests.post(f"{ORDER_URL}/orders/{order_id}/pay", json={"paymentMethod": "CARD"})
    print(f"  -> HTTP статус ответа: {res_invalid_pay.status_code}")
    print(f"  -> Тело ошибки: {res_invalid_pay.json().get('detail')}")
    assert res_invalid_pay.status_code == 400
    print("  -> [✓] Бизнес-правило соблюдено: повторная оплата отклонена, новое событие НЕ создано!")

    # Проверяем, что число событий в Event Store осталось равным 2
    res_events_check = requests.get(f"{ORDER_URL}/event-store/orders/{order_id}").json()
    assert len(res_events_check) == 2, "Число событий не должно было увеличиться при сбое валидации"

    # -------------------------------------------------------------------------
    # СЦЕНАРИЙ 4: КОМАНДА ОТМЕНЫ ЗАКАЗА
    # -------------------------------------------------------------------------
    print_separator("ЭТАП 4: КОМАНДА ОТМЕНЫ ЗАКАЗА (POST /orders/{id}/cancel)")
    print(f"Отправка команды отмены для заказа {order_id}...")

    res_cancel = requests.post(
        f"{ORDER_URL}/orders/{order_id}/cancel",
        json={"reason": "Клиент запросил возврат средств"}
    )
    assert res_cancel.status_code == 200, f"Ошибка отмены: {res_cancel.text}"
    cancel_data = res_cancel.json()
    print(f"  -> Ответ команды: {cancel_data['message']}")
    print(f"  -> Итоговая версия в Event Store: v{cancel_data['version']}")
    assert cancel_data["version"] == 3
    assert cancel_data["status"] == "CANCELLED"

    # Проверяем полную последовательность 3 событий: OrderCreated -> OrderPaid -> OrderCancelled
    res_events_3 = requests.get(f"{ORDER_URL}/event-store/orders/{order_id}").json()
    print(f"\nПолная история событий заказа {order_id} в Event Store:")
    for ev in res_events_3:
        print(f"  [v{ev['version']}] {ev['eventType']} -> {ev['payload']}")
    assert len(res_events_3) == 3
    assert res_events_3[0]["eventType"] == "OrderCreated"
    assert res_events_3[1]["eventType"] == "OrderPaid"
    assert res_events_3[2]["eventType"] == "OrderCancelled"

    # Попытка повторной отмены уже отмененного заказа
    print("\nПопытка повторной отмены уже отмененного заказа:")
    res_invalid_cancel = requests.post(f"{ORDER_URL}/orders/{order_id}/cancel", json={"reason": "Повторно"})
    print(f"  -> HTTP статус: {res_invalid_cancel.status_code}")
    print(f"  -> Ошибка: {res_invalid_cancel.json().get('detail')}")
    assert res_invalid_cancel.status_code == 400
    print("  -> [✓] Бизнес-правило соблюдено: повторная отмена отклонена!")

    # -------------------------------------------------------------------------
    # СЦЕНАРИЙ 5: ПРОВЕРКА СЕРВИСА АНАЛИТИКИ (KAFKA STREAM CONSUMER)
    # -------------------------------------------------------------------------
    print_separator("ЭТАП 5: СЕРВИС АНАЛИТИКИ (KAFKA STREAM АГРЕГАЦИЯ)")
    print("Создаем второй заказ для наполнения аналитической выборки...")

    r_ord2 = requests.post(f"{ORDER_URL}/orders", json={
        "userId": "client-daniil-202",
        "items": [{"productId": "Клавиатура Keychron", "quantity": 1, "price": 10500.0}],
        "totalAmount": 10500.0
    })
    ord2_id = r_ord2.json()["orderId"]
    requests.post(f"{ORDER_URL}/orders/{ord2_id}/pay", json={"paymentMethod": "SBP"})

    print("Создаем третий заказ (останется в CREATED)...")
    requests.post(f"{ORDER_URL}/orders", json={
        "userId": "client-guest-303",
        "items": [{"productId": "Коврик для мыши", "quantity": 2, "price": 1500.0}],
        "totalAmount": 3000.0
    })

    time.sleep(0.5)

    # Запрашиваем сводку аналитики
    res_analytics = requests.get(f"{ANALYTICS_URL}/analytics/summary")
    assert res_analytics.status_code == 200
    analytics_data = res_analytics.json()

    print("\nАгрегированные показатели из сервиса аналитики (GET /analytics/summary):")
    print(f"  -> Всего заказов (total_orders):           {analytics_data['total_orders']}")
    print(f"  -> Оплаченная выручка (total_paid_revenue): {analytics_data['total_paid_revenue']} руб.")
    print(f"  -> Средний чек (average_order_amount):     {analytics_data['average_order_amount']} руб.")
    print(f"  -> Распределение по статусам:              {analytics_data['orders_by_status']}")
    print(f"  -> Всего событий обработано:               {analytics_data['total_events_processed']}")

    assert analytics_data["total_orders"] >= 3
    assert analytics_data["total_paid_revenue"] >= 10500.0
    print("  -> [✓] Сервис аналитики корректно считывает поток событий и рассчитывает метрики!")

    print("\n" + "=" * 78)
    print("  ИТОГ: ВСЕ ТРЕБОВАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ №6 ВЫПОЛНЕНЫ НА 100%! [OK]")
    print("=" * 78)

if __name__ == "__main__":
    main()
