import os
import sys
import time
import subprocess

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    print("=" * 75)
    print("  ЛОКАЛЬНЫЙ ЗАПУСК ЛАБОРАТОРНОЙ РАБОТЫ №6 (CQRS & EVENT SOURCING)")
    print("=" * 75)

    env = os.environ.copy()
    env["PYTHONPATH"] = CURRENT_DIR
    env["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
    env["KAFKA_TOPIC"] = "orders-events"
    env["EVENT_STORE_DB"] = os.path.join(CURRENT_DIR, "event_store.db")
    env["ANALYTICS_SERVICE_URL"] = "http://127.0.0.1:8002/internal/events"

    processes = []

    try:
        # 1. Запуск Analytics Service
        print("\n[1/2] Запуск Analytics Service (порт 8002)...")
        p_analytics = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn",
                "analytics_service.app:app",
                "--host", "127.0.0.1",
                "--port", "8002"
            ],
            env=env,
            cwd=CURRENT_DIR
        )
        processes.append(("Analytics Service", p_analytics))
        time.sleep(1.0)

        # 2. Запуск Order Service (Write & Read Side)
        print("[2/2] Запуск Order Service (порт 8001)...")
        p_order = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn",
                "order_service.app:app",
                "--host", "127.0.0.1",
                "--port", "8001"
            ],
            env=env,
            cwd=CURRENT_DIR
        )
        processes.append(("Order Service", p_order))
        time.sleep(1.5)

        print("\n" + "=" * 75)
        print("  СЕРВИСЫ УСПЕШНО ЗАПУЩЕНЫ!")
        print("  -> Order Service Dashboard:     http://localhost:8001/")
        print("  -> Analytics Service Dashboard: http://localhost:8002/")
        print("  -> Swagger / OpenAPI:           http://localhost:8001/docs")
        print("  -> Запуск автотестов:           python test_cqrs_es.py")
        print("=" * 75)
        print("\nНажмите Ctrl+C для плавной остановки сервисов...\n")

        while True:
            time.sleep(1)
            for name, proc in processes:
                poll = proc.poll()
                if poll is not None:
                    print(f"Внимание: процесс {name} неожиданно завершился с кодом {poll}")
                    return

    except KeyboardInterrupt:
        print("\nОстановка сервисов...")
    finally:
        for name, proc in processes:
            print(f"Останавливаю {name} (PID {proc.pid})...")
            try:
                proc.terminate()
                proc.wait(timeout=3)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        print("Все сервисы остановлены.")

if __name__ == "__main__":
    main()
