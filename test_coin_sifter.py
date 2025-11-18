import requests
import os
import json
import io
import csv
from datetime import datetime # [NEW] Для логирования времени запуска

# --- КОНФИГУРАЦИЯ ---
# URL и Токен, которые вы предоставили.
# (В реальном приложении лучше использовать переменные окружения: os.getenv("COIN_SIFTER_URL"))
BASE_URL = "https://coin-sifter-server.onrender.com"
SECRET_TOKEN = "O0hrTGEd3meImdof/H0Hj2XOKuVgQAbr+D9w0DRZvtA="

# [cite_start]Добавляем префикс /api/v1, как указано в файле endpoints_description.txt [cite: 9454, 9486, 9498, 9506]
API_URL = f"{BASE_URL}"

# [cite_start]Заголовки для аутентификации [cite: 11907-11908]
HEADERS = {
    "X-Auth-Token": SECRET_TOKEN,
    "Content-Type": "application/json"
}

def print_header(title):
    """Печатает красивый заголовок для раздела теста."""
    print("\n" + "="*70)
    print(f"🧪 ТЕСТ: {title}")
    print("="*70)

def test_get_filtered_json():
    """
    Тестирует GET /api/v1/coins/filtered
    1. [cite_start]Получает все монеты (JSON) [cite: 9486]
    2. Считает их
    3. Показывает образец (первую монету)
    4. [FIX] Проверяет структуру ответа на соответствие спецификации test_api_v3_e2e.py [cite: 11698-11717]
    """
    print_header("GET /api/v1/coins/filtered (Полные данные JSON)")
    endpoint_url = f"{API_URL}/coins/filtered" # [cite: 9486]

    try:
        response = requests.get(endpoint_url, headers=HEADERS, timeout=30)
        
        # 1. Проверка статуса
        if response.status_code == 200:
            print(f"✅ (200 OK) Успешно получили ответ.")
            
            # 2. [FIX] Проверить, что ответ - это JSON, иначе будет ошибка
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"❌ ОШИБКА: Ответ не является корректным JSON.")
                print(f"   Тело ответа: {response.text[:500]}")
                return

            # [FIX] Проверяем структуру ответа на основе test_api_v3_e2e.py [cite: 11698-11717]
            # Ожидаемые ключи: 'count', 'coins', 'excluded_count', 'excluded_coins'
            expected_keys = {'count', 'coins', 'excluded_count', 'excluded_coins'}
            if not all(key in data for key in expected_keys):
                print(f"❌ ОШИБКА: Ответ JSON не содержит всех ожидаемых ключей {expected_keys}.")
                print(f"   Полученные ключи: {list(data.keys())}")
                return

            count = data.get('count') # [cite: 11817]
            coins = data.get('coins') # [cite: 11821]
            excluded_count = data.get('excluded_count')
            excluded_coins = data.get('excluded_coins')

            # [FIX] Проверяем типы данных
            if not isinstance(count, int) or not isinstance(coins, list):
                print(f"❌ ОШИБКА: 'count' должен быть int, 'coins' должен быть list.")
                print(f"   Фактические типы: count={type(count)}, coins={type(coins)}")
                return

            print(f"📊 Найдено монет: {count}")
            print(f"📊 Исключено монет: {excluded_count}") # [NEW] Выводим исключённые
            
            # 3. Дать образец данных
            if count > 0:
                print("\n📋 Образец данных (первая монета):")
                # Используем json.dumps для красивого вывода
                sample_coin = coins[0]
                print(json.dumps(sample_coin, indent=2, ensure_ascii=False))
                
                # [cite_start]Проверка ключевых полей на основе test_api_v3_e2e.py [cite: 11698-11717]
                print(f"\n🔍 Проверка ключевых полей образца:")
                # [FIX] Проверяем наличие полей, а не просто печатаем
                required_coin_fields = [
                    'symbol', 'full_symbol', 'exchanges', 'volume_24h_usd', 'hurst_1h', 
                    'hurst_4h', 'hurst_1d', 'jaggedness_1h', 'jaggedness_4h', 'jaggedness_1d',
                    'price_change_percentage_1h', 'price_change_percentage_24h', 'price_change_percentage_7d',
                    'price_change_percentage_1y', 'trend_1h', 'trend_4h', 'trend_1d'
                ]
                missing_fields = [field for field in required_coin_fields if field not in sample_coin]
                if missing_fields:
                    print(f"   ❌ НЕКОТОРЫЕ ОЖИДАЕМЫЕ ПОЛЯ ОТСУТСТВУЮТ: {missing_fields}")
                else:
                    print("   ✅ Все ожидаемые поля присутствуют.")
                
                for field in ['symbol', 'full_symbol', 'exchanges', 'volume_24h_usd', 'hurst_1h']:
                    print(f"     - '{field}': {sample_coin.get(field)}")
            else:
                print("⚠️ В ответе 0 монет. (Возможно, анализ еще не завершен?)")
            
            # [NEW] Проверка исключённых монет
            if excluded_count > 0 and excluded_coins:
                print(f"\n📋 Образец исключённых монет (первая):")
                print(json.dumps(excluded_coins[0], indent=2, ensure_ascii=False))

        else:
            print(f"❌ ОШИБКА: Неверный статус-код: {response.status_code}")
            print(f"   Ответ: {response.text[:200]}")

    except requests.exceptions.Timeout:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА (Timeout): Запрос превысил время ожидания.")
    except requests.exceptions.RequestException as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА (RequestException): {e}")

def test_get_formatted_symbols():
    """
    Тестирует GET /api/v1/coins/formatted-symbols
    1. [cite_start]Получает отформатированные символы [cite: 9506]
    2. Считает их
    3. Показывает образец (первые 5)
    4. [FIX] Проверяет структуру ответа на соответствие спецификации test_api_v3_e2e.py [cite: 11840-11882]
    """
    print_header("GET /api/v1/coins/formatted-symbols (Для TradingView)")
    endpoint_url = f"{API_URL}/coins/formatted-symbols" # [cite: 9506]

    try:
        response = requests.get(endpoint_url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            print(f"✅ (200 OK) Успешно получили ответ.")
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"❌ ОШИБКА: Ответ не является корректным JSON.")
                print(f"   Тело ответа: {response.text[:500]}")
                return

            # [FIX] Проверяем структуру ответа на основе test_api_v3_e2e.py [cite: 11840-11882]
            expected_keys = {'count', 'symbols'}
            if not all(key in data for key in expected_keys):
                print(f"❌ ОШИБКА: Ответ JSON не содержит всех ожидаемых ключей {expected_keys}.")
                print(f"   Полученные ключи: {list(data.keys())}")
                return

            count = data.get('count') # [cite: 11817]
            symbols = data.get('symbols') # [cite: 11821]

            # [FIX] Проверяем типы данных
            if not isinstance(count, int) or not isinstance(symbols, list):
                print(f"❌ ОШИБКА: 'count' должен быть int, 'symbols' должен быть list.")
                print(f"   Фактические типы: count={type(count)}, symbols={type(symbols)}")
                return

            print(f"📊 Найдено символов: {count}")
            
            if count > 0:
                print("\n📋 Образец данных (первые 5):")
                # [FIX] Проверяем структуру элементов списка 'symbols' на основе test_api_v3_e2e.py [cite: 11854-11864]
                for i, item in enumerate(symbols[:5]):
                    if not isinstance(item, dict):
                        print(f"   ❌ Элемент {i} в 'symbols' не является объектом (dict).")
                        continue
                    
                    required_symbol_fields = ['symbol', 'exchanges']
                    missing_fields = [field for field in required_symbol_fields if field not in item]
                    if missing_fields:
                        print(f"   ❌ Элемент {i} не содержит всех ожидаемых полей: {missing_fields}")
                        continue

                    print(f"  - #{i+1}: Символ: {item.get('symbol')}, Биржи: {item.get('exchanges')}")
            else:
                print("⚠️ В ответе 0 символов.")
        else:
            print(f"❌ ОШИБКА: Неверный статус-код: {response.status_code}")
            print(f"   Ответ: {response.text[:200]}")

    except requests.exceptions.Timeout:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА (Timeout): Запрос превысил время ожидания.")
    except requests.exceptions.RequestException as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА (RequestException): {e}")

def test_get_filtered_csv():
    """
    Тестирует GET /api/v1/coins/filtered/csv
    1. [cite_start]Получает CSV файл [cite: 9498]
    2. Показывает образец (первые 5 строк)
    3. [FIX] Проверяет, что сервер вернул именно CSV
    4. [FIX] Обрабатывает ошибки при парсинге CSV
    """
    print_header("GET /api/v1/coins/filtered/csv (Данные в CSV)")
    endpoint_url = f"{API_URL}/coins/filtered/csv" # [cite: 9498]

    try:
        # Этот эндпоинт публичный, токен не обязателен, но мы его отправим.
        response = requests.get(endpoint_url, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '')
            print(f"✅ (200 OK) Успешно получили ответ.")
            print(f"   Content-Type: {content_type}")

            # [cite_start]Проверяем, что сервер вернул именно CSV [cite: 11753-11755]
            if 'text/csv' in content_type:
                print("✅ Content-Type корректный ('text/csv').")
                
                # 2. Посчитать и дать образец
                try:
                    content = response.content.decode('utf-8')
                except UnicodeDecodeError:
                    print(f"❌ ОШИБКА: Ответ не может быть декодирован как UTF-8.")
                    return

                lines = content.splitlines()
                
                # Используем csv.reader для корректного подсчета строк, 
                # даже если данные содержат переносы строк в кавычках.
                try:
                    reader = csv.reader(io.StringIO(content))
                    # [FIX] Подсчитываем только строки данных, исключая заголовок
                    headers = next(reader, None) # Читаем заголовок
                    if headers:
                        print(f"   Заголовки CSV: {headers}")
                    row_count = sum(1 for row in reader) # Подсчитываем остальные строки
                except csv.Error as e:
                    print(f"❌ ОШИБКА при парсинге CSV: {e}")
                    return
                
                print(f"📊 Найдено строк (монет): {row_count}")
                
                if row_count > 0 and len(lines) > 1: # Убедимся, что есть заголовок и данные
                    print("\n📋 Образец данных (первые 5 строк CSV, включая заголовок):")
                    for i, line in enumerate(lines[:6]): # Включаем заголовок
                        # Обрезаем, чтобы не выводить слишком длинные строки
                        print(f"  {i}: {line[:150]}...")
            else:
                print(f"❌ ОШИБКА: Неверный Content-Type. Ожидался 'text/csv'.")
        
        else:
            print(f"❌ ОШИБКА: Неверный статус-код: {response.status_code}")
            print(f"   Ответ: {response.text[:200]}")

    except requests.exceptions.Timeout:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА (Timeout): Запрос превысил время ожидания.")
    except requests.exceptions.RequestException as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА (RequestException): {e}")

if __name__ == "__main__":
    print("🚀 Запуск тестов для эндпоинтов CoinSifter...")
    print(f"   URL: {API_URL}")
    print(f"   Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. Тест JSON эндпоинта (основной запрос)
    test_get_filtered_json()
    
    # 2. Тест эндпоинта для TradingView
    test_get_formatted_symbols()
    
    # 3. Тест CSV эндпоинта
    test_get_filtered_csv()
    
    print("\n" + "="*70)
    print("🏁 Тестирование завершено.")
    print("="*70)
