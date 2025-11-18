import requests
import json
import os
import sys

# --- Configuration ---
# 🔴 УКАЖИ URL СЕРВЕРА, КОТОРЫЙ ХОЧЕШЬ ПРОТЕСТИРОВАТЬ:
BASE_URL = "https://bizzar-kline-data-fetcher.onrender.com"

# Твой Секретный Токен
SECRET_TOKEN = "O0hrTGEd3meImdof/H0Hj2XOKuVgQAbr+D9w0DRZvtA="

# Эндпоинты
ENDPOINT_BASE_TASK = "/internal/update-base-data"
ENDPOINT_TARGET_TASK = "/internal/generate-target"

# ✅ ИСПРАВЛЕНО: Заголовки для X-API-Key авторизации
HEADERS = {
    "X-API-Key": SECRET_TOKEN,  # <-- ВОТ ТАК!
    "Content-Type": "application/json",
}


def run_base_task():
    """Запускает сбор базового таймфрейма (4h или 12h)."""
    print("="*60)
    print("🔥 Запуск Базовой Задачи (Base Task)")
    print("="*60)
    print(f"Сервер: {BASE_URL}")
    print(f"Эндпоинт: POST {ENDPOINT_BASE_TASK}")
    print(f"Авторизация: X-API-Key")
    print("="*60)
    
    try:
        url = BASE_URL + ENDPOINT_BASE_TASK
        
        # Таймаут 5 минут (задача может выполняться долго)
        print("\n⏳ Отправка запроса... (таймаут 300 сек)")
        response = requests.post(url, headers=HEADERS, timeout=300)
        
        print(f"\n📡 Получен ответ: HTTP {response.status_code}")
        
        if response.status_code == 200:
            print("\n✅ УСПЕХ: Базовая задача успешно выполнена!")
            try:
                print("\n--- Ответ сервера ---")
                print(json.dumps(response.json(), indent=2, ensure_ascii=False))
            except requests.exceptions.JSONDecodeError:
                print(f"Ответ (raw): {response.text}")
        
        elif response.status_code == 403:
            print("\n❌ ОШИБКА: 403 Forbidden - Неверный API Key!")
            print("   Проверь:")
            print("   1. Значение SECRET_TOKEN в скрипте совпадает с сервером")
            print("   2. В api_routes.py используется verify_api_key")
            print(f"\n   Ответ: {response.text}")
        
        elif response.status_code == 404:
            print("\n❌ ОШИБКА: 404 Not Found - Эндпоинт не найден!")
            print("   Убедись, что:")
            print("   1. URL сервера правильный")
            print("   2. В api_routes.py ЕСТЬ эндпоинт /internal/update-base-data")
            print("   3. Нет лишних префиксов (типа /api/v1)")
        
        elif response.status_code == 503:
            print("\n⚠️  ОШИБКА: 503 Service Unavailable - Сервис недоступен")
            print("   Возможные причины:")
            print("   1. Redis не подключен")
            print("   2. Не удалось получить список монет")
            print(f"\n   Ответ: {response.text}")
        
        else:
            print(f"\n❌ ОШИБКА: Не удалось запустить задачу")
            print(f"   Статус: {response.status_code}")
            print(f"   Ответ: {response.text}")

    except requests.exceptions.Timeout:
        print("\n⏰ ТАЙМАУТ: Сервер не ответил за 5 минут")
        print("   Это может быть нормально, если задача выполняется дольше.")
        print("   Проверь логи сервера, чтобы увидеть, завершилась ли она.")
    
    except requests.exceptions.ConnectionError as e:
        print(f"\n❌ ОШИБКА: Не удалось подключиться к серверу")
        print(f"   {e}")
        print("\n   Проверь:")
        print("   1. Сервер запущен")
        print("   2. URL правильный")
        print("   3. Нет проблем с сетью")
    
    except requests.exceptions.RequestException as e:
        print(f"\n❌ ОШИБКА: Проблема с запросом: {e}")
    
    except Exception as e:
        print(f"\n❌ Неизвестная ошибка: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*60)
    print("🏁 Скрипт завершен")
    print("="*60)


def run_target_task():
    """Запускает генерацию целевого таймфрейма (8h или 1d)."""
    print("="*60)
    print("🔥 Запуск Целевой Задачи (Target Task)")
    print("="*60)
    print(f"Сервер: {BASE_URL}")
    print(f"Эндпоинт: POST {ENDPOINT_TARGET_TASK}")
    print(f"Авторизация: X-API-Key")
    print("="*60)
    
    try:
        url = BASE_URL + ENDPOINT_TARGET_TASK
        
        print("\n⏳ Отправка запроса... (таймаут 300 сек)")
        response = requests.post(url, headers=HEADERS, timeout=300)
        
        print(f"\n📡 Получен ответ: HTTP {response.status_code}")
        
        if response.status_code == 200:
            print("\n✅ УСПЕХ: Целевая задача успешно выполнена!")
            try:
                print("\n--- Ответ сервера ---")
                print(json.dumps(response.json(), indent=2, ensure_ascii=False))
            except requests.exceptions.JSONDecodeError:
                print(f"Ответ (raw): {response.text}")
        
        elif response.status_code == 403:
            print("\n❌ ОШИБКА: 403 Forbidden - Неверный API Key!")
            print(f"\n   Ответ: {response.text}")
        
        elif response.status_code == 404:
            print("\n❌ ОШИБКА: 404 Not Found - Эндпоинт не найден!")
        
        else:
            print(f"\n❌ ОШИБКА: Статус {response.status_code}")
            print(f"   Ответ: {response.text}")

    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")

    print("\n" + "="*60)
    print("🏁 Скрипт завершен")
    print("="*60)


if __name__ == "__main__":
    # Проверка конфигурации
    if not SECRET_TOKEN:
        print("❌ ОШИБКА: SECRET_TOKEN не установлен в скрипте.")
        sys.exit(1)
        
    if "your-project-name" in BASE_URL:
        print("❌ ОШИБКА: Пожалуйста, отредактируй BASE_URL в скрипте.")
        sys.exit(1)
    
    # Выбор задачи
    print("\nВыбери задачу:")
    print("1. Базовая задача (update-base-data)")
    print("2. Целевая задача (generate-target)")
    print("3. Обе задачи последовательно")
    
    choice = input("\nВведи номер (1/2/3) или Enter для базовой: ").strip()
    
    if choice == "2":
        run_target_task()
    elif choice == "3":
        run_base_task()
        print("\n\n")
        input("Нажми Enter для запуска целевой задачи...")
        run_target_task()
    else:
        run_base_task()