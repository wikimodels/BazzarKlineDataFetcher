# test_redis.py
import asyncio
from cache_manager import get_redis_connection

async def test():
    redis = await get_redis_connection()
    
    # Получаем все ключи cache:*
    keys = await redis.keys('cache:*')
    print(f'Всего ключей cache:*: {len(keys)}')
    print(f'Ключи: {keys}')
    
    # Проверяем конкретные ключи
    for key_name in ['cache:4h', 'cache:8h', 'cache:1h']:
        data = await redis.get(key_name)
        if data:
            print(f'✅ {key_name}: {len(data)} байт')
        else:
            print(f'❌ {key_name}: НЕТ ДАННЫХ')

asyncio.run(test())