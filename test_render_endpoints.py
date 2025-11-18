import asyncio
import httpx
import os
import sys
from dotenv import load_dotenv
import logging

# Настройка логгера
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MANUAL_TEST")

# 1. Загрузка конфигурации
load_dotenv()

# Вы можете поменять URL на адрес вашего Render сервиса, если тестируете удаленно
# Например: BASE_URL = "https://ваш-проект.onrender.com"
BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8000") 
SECRET_TOKEN = os.environ.get("SECRET_TOKEN")

if not SECRET_TOKEN:
    logger.critical("💥 ОШИБКА: SECRET_TOKEN не найден в .env файле!")
    sys.exit(1)

async def trigger_endpoint(client: httpx.AsyncClient, endpoint: str, description: str):
    """
    Отправляет POST запрос на эндпоинт с правильным заголовком X-API-Key.
    """
    url = f"{endpoint}"
    
    # ВАЖНО: Используем новый заголовок, как в api_routes.py
    headers = {
        "X-API-Key": SECRET_TOKEN,
        "Content-Type": "application/json"
    }
    
    logger.info(f"--- 🚀 Запуск: {description} ({endpoint}) ---")
    
    try:
        response = await client.post(url, headers=headers, timeout=60.0)
        
        if response.status_code == 200:
            logger.info(f"✅ УСПЕХ (200 OK): {response.json()}")
        elif response.status_code == 409:
            logger.warning(f"⚠️ Блокировка занята (409): Воркер уже работает.")
        elif response.status_code == 403:
            logger.error(f"❌ ДОСТУП ЗАПРЕЩЕН (403): Неверный токен. Проверьте SECRET_TOKEN.")
        elif response.status_code == 422:
            logger.error(f"❌ ОШИБКА ВАЛИДАЦИИ (422): Сервер не увидел заголовок X-API-Key.")
            logger.error(f"Тело ответа: {response.text}")
        else:
            logger.error(f"❌ ОШИБКА ({response.status_code}): {response.text}")
            
    except Exception as e:
        logger.error(f"💥 Ошибка сети: {e}")

async def main():
    logger.info(f"Цель: {BASE_URL}")
    logger.info(f"Токен: {SECRET_TOKEN[:4]}***... (X-API-Key)")
    
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=120.0) as client:
        
        # 1. Проверка здоровья (Health Check)
        try:
            resp = await client.get("/health")
            logger.info(f"🏥 Health Check: {resp.status_code} {resp.text}")
        except Exception as e:
            logger.critical(f"💀 Сервер недоступен: {e}")
            return

        print("\n")

        # 2. Эндпоинт 1H (Тот самый, что выдавал 422)
        await trigger_endpoint(
            client, 
            "/internal/update-1h-and-check-alerts", 
            "Сбор 1H и Алерты"
        )

        print("\n")

        # 3. Эндпоинт Base (12H или 4H)
        await trigger_endpoint(
            client, 
            "/internal/update-base-data", 
            "Сбор Base-TF"
        )

        print("\n")

        # 4. Эндпоинт Target (1D или 8H)
        await trigger_endpoint(
            client, 
            "/internal/generate-target", 
            "Генерация Target-TF"
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nТест прерван.")