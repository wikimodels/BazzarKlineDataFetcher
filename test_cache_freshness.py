#!/usr/bin/env python
# check_all_cache_freshness.py
"""
Этот скрипт подключается ко всем настроенным базам данных Redis (Default, I, II),
ищет все ключи кэша (cache:*) и проверяет их "свежесть" (актуальность)
на основе поля 'closeTime' внутри JSON.
"""

import asyncio
import logging
import sys
import os
import time
import re
from dotenv import load_dotenv
from typing import Dict, Any, Optional

# --- 1. Настройка пути и импорт модулей проекта ---
# Добавляем корневую папку проекта в sys.path, чтобы найти cache_manager
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from cache_manager import get_redis_connection, load_from_cache
except ImportError as e:
    print(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось импортировать 'cache_manager'.", file=sys.stderr)
    print("Убедитесь, что вы запускаете этот скрипт из корневой папки проекта.", file=sys.stderr)
    print(f"Ошибка: {e}", file=sys.stderr)
    sys.exit(1)

# --- 2. Загрузка .env ---
load_dotenv()

# --- 3. Настройки теста ---
# Список имен соединений, которые мы будем проверять.
# --- ИЗМЕНЕНИЕ: Удалена база "II" из списка проверки ---
DATABASES_TO_CHECK = ["default", "I"]
# ----------------------------------------------------

# Период отсрочки (15 минут), как в test_cache_freshness.py 
GRACE_PERIOD_MS = 15 * 60 * 1000

# Регулярное выражение для извлечения таймфрейма (1h, 4h, 12h, 1d) из ключа
CACHE_KEY_REGEX = re.compile(r"cache:([1-9][0-9]*[hdwm]|1d)")

# --- 4. Настройка логгера ---
# ANSI цвета
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"
CYAN = "\033[96m"

class ColoredFormatter(logging.Formatter):
    """Форматтер для цветного вывода логов в терминал."""
    LEVEL_COLORS = {
        logging.INFO: GREEN,
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
    }
    
    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, "")
        timestamp = f"{CYAN}[{time.strftime('%H:%M:%S')}] (CACHE_CHECK) - {color}"
        # Мы используем %(message)s напрямую, т.к. логгер настроен ниже
        message = super().format(record)
        return f"{timestamp}{message}{RESET}"

def setup_colored_logger() -> logging.Logger:
    log = logging.getLogger("CACHE_FRESHNESS_CHECKER")
    log.setLevel(logging.INFO)
    
    if log.hasHandlers():
        log.handlers.clear()
    
    handler = logging.StreamHandler(sys.stdout)
    # Устанавливаем форматтер так, чтобы он выводил только сообщение
    formatter = ColoredFormatter('%(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log

log = setup_colored_logger()

# --- 5. Вспомогательные функции ---

def get_interval_duration_ms(interval: str) -> int:
    """
    Возвращает длительность интервала в миллисекундах.
    (Скопировано из test_cache_freshness.py)
    """
    duration_map = {
        '1h': 60 * 60 * 1000,
        '4h': 4 * 60 * 60 * 1000,
        '8h': 8 * 60 * 60 * 1000,
        '12h': 12 * 60 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
        # Добавим 'w' и 'm' на всякий случай
        '1w': 7 * 24 * 60 * 1000,
        '1m': 30 * 24 * 60 * 1000,
    }
    return duration_map.get(interval.lower(), 0)

def _get_timeframe_from_key(key_str: str) -> Optional[str]:
    """Извлекает таймфрейм (1h, 4h...) из строки ключа (cache:4h)."""
    match = CACHE_KEY_REGEX.search(key_str)
    if match:
        return match.group(1)
    return None

# --- 6. Основная логика ---

async def check_database(db_name: str) -> bool:
    """
    Подключается к указанной БД, находит все ключи cache:*
    и проверяет их свежесть.
    """
    log.info(f"\n{'='*60}")
    log.info(f"--- 🔎 Подключение к Базе Данных: '{db_name}' ---")
    
    redis_conn = None
    all_keys_valid = True
    
    try:
        # 1. Подключение
        redis_conn = await get_redis_connection(name=db_name)
        if not redis_conn:
            log.error(f"💥 [FAIL] Не удалось подключиться к БД '{db_name}'. Проверьте .env (URL/Token).")
            return False
        
        log.info(f"✅ [OK] Успешно подключено к БД '{db_name}'.")

        # 2. Поиск ключей
        log.info(f"Ищу ключи по маске 'cache:*'...")
        try:
            cache_keys_bytes = await redis_conn.keys("cache:*")
        except Exception as e:
            log.error(f"💥 [FAIL] Ошибка при выполнении .keys('cache:*') в БД '{db_name}': {e}")
            return False
            
        cache_keys = [k.decode('utf-8') for k in cache_keys_bytes]
        
        if not cache_keys:
            log.warning(f"⚠️  [WARN] В БД '{db_name}' не найдено ключей по маске 'cache:*'.")
            return True # Технически не провал, просто нет данных

        log.info(f"Найдено {len(cache_keys)} ключей: {cache_keys}")

        # 3. Проверка каждого ключа
        current_utc_time_ms = int(time.time() * 1000)
        
        for key_str in cache_keys:
            log.info(f"--- 🔬 Проверяю ключ: '{key_str}' ---")
            
            timeframe = _get_timeframe_from_key(key_str)
            if not timeframe:
                log.warning(f"       ⚠️  [WARN] Не удалось извлечь таймфрейм из '{key_str}'. Ключ пропущен.")
                continue

            interval_ms = get_interval_duration_ms(timeframe)
            if interval_ms == 0:
                log.warning(f"       ⚠️  [WARN] Неизвестный интервал для таймфрейма '{timeframe}'. Ключ пропущен.")
                continue
            
            # Загружаем данные из кэша (load_from_cache ожидает ключ БЕЗ префикса 'cache:')
            try:
                data = await load_from_cache(timeframe, redis_conn)
            except Exception as e:
                log.error(f"       💥 [FAIL] Ошибка при загрузке/декомпрессии данных '{key_str}': {e}", exc_info=True)
                all_keys_valid = False
                continue

            if not data:
                log.error(f"       💥 [FAIL] Ключ '{key_str}' пуст или содержит некорректные данные (None).")
                all_keys_valid = False
                continue

            # Проверяем свежесть по 'closeTime' 
            last_close_time_ms = data.get("closeTime")
            if not last_close_time_ms or not isinstance(last_close_time_ms, int):
                log.error(f"       💥 [FAIL] Ключ '{key_str}' не содержит корректного 'closeTime' в корне JSON.")
                all_keys_valid = False
                continue

            # Логика проверки 
            allowed_staleness_ms = interval_ms + GRACE_PERIOD_MS
            time_diff_ms = current_utc_time_ms - last_close_time_ms
            time_diff_hours = time_diff_ms / 3600000

            if time_diff_ms < 0:
                log.error(f"       💥 [FAIL] Ключ '{key_str}' из будущего? (Разница: {time_diff_hours:.1f} ч).")
                all_keys_valid = False
            
            elif time_diff_ms <= allowed_staleness_ms:
                log.info(f"       ✅ [OK] Ключ '{key_str}' актуален (Данные: {time_diff_hours:.1f} ч назад).")
            
            else:
                log.error(f"       💥 [FAIL] Ключ '{key_str}' ПРОТУХ!")
                log.error(f"       Последние данные: {time_diff_hours:.1f} ч назад.")
                log.error(f"       Допустимо (интервал + буфер {GRACE_PERIOD_MS / 60000:.0f} мин): {allowed_staleness_ms / 3600000:.1f} ч назад.")
                all_keys_valid = False

    except Exception as e:
        log.error(f"💥 [FAIL] КРИТИЧЕСКАЯ ОШИБКА при проверке БД '{db_name}': {e}", exc_info=True)
        all_keys_valid = False
        
    finally:
        if redis_conn:
            # Важно закрыть соединение, так как мы управляем им вручную
            await redis_conn.aclose()
            log.info(f"---  disconnection --- Соединение с '{db_name}' закрыто.")
            
    return all_keys_valid

async def main():
    """
    Главный оркестратор.
    """
    log.info("--- 🚀 ЗАПУСК СКРИПТА ПРОВЕРКИ СВЕЖЕСТИ ВСЕХ КЭШЕЙ ---")
    
    results = {}
    
    for db_name in DATABASES_TO_CHECK:
        results[db_name] = await check_database(db_name)
    
    log.info("\n" + "="*60)
    log.info("--- 📊 ИТОГОВЫЙ ОТЧЁТ ---")
    
    final_success = True
    for db_name, success in results.items():
        if success:
            log.info(f"  {GREEN}✅ {db_name}: Все проверки пройдены.{RESET}")
        else:
            log.error(f"  {RED}💥 {db_name}: Обнаружены ошибки.{RESET}")
            final_success = False
            
    log.info("="*60)
    
    if final_success:
        log.info(f"{GREEN}--- 🏆🏆🏆 ВСЕ КЭШИ ВО ВСЕХ БД АКТУАЛЬНЫ. ---{RESET}")
    else:
        log.error(f"{RED}--- 💥 ОБНАРУЖЕНЫ ПРОБЛЕМЫ С АКТУАЛЬНОСТЬЮ КЭШЕЙ. ---{RESET}")
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.warning("\n\nПроверка прервана пользователем.")
        sys.exit(2)
    except Exception as e:
        log.error(f"\n\n💥 Непредвиденная ошибка в main: {e}", exc_info=True)
        sys.exit(1)