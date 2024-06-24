import redis
import os
import time
import json
import uuid

# Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Unique id of a worker-а (PID could be used or UUID could be generated)
#worker_id = str(uuid.uuid4())
worker_id = 1

def select_date(worker_id, storage):
    print(f"#worker pid:{os.getpid()}")

    """
    Selects a date from a list of dates in Redis and reserves it with unique id of a worker-а.
    """
    dates = json.loads(redis_client.get(storage))
    
    for date in dates:
        # Attempt to set a flag for one of the dates in Redis with worker unique id
        if redis_client.setnx(f'worker:date:{date}:{worker_id}', os.getpid()):
            # If the date has been reserved sucessfully, return it
            return date
    return None

def release_date(date, worker_id):
    """
    Releases date in Redis, so that another worker could use it.
    """
    redis_client.delete(f'worker:date:{date}:{worker_id}')

def check_date_validity(date, storage):
    """
    Checks if the date is valid according to the Redis.
    """
    dates = json.loads(redis_client.get(storage))
    print(dates, type(dates[0]), type(date))
    return date in dates

"""
# Пример использования
# Попытки выбора даты с задержкой
selected_date = None
while not selected_date:
    selected_date = select_date(worker_id)
    if not selected_date:
        time.sleep(1)  # Задержка перед повторной попыткой

if selected_date:
    print(f'Worker {worker_id} выбрал дату {selected_date}')
    # Ваша основная логика здесь
    while check_date_validity(selected_date):
        print(f'Дата {selected_date} всё ещё актуальна для worker-а {worker_id}')
        time.sleep(10)  # Проверяем каждые 10 секунд
    print(f'Дата {selected_date} больше не актуальна для worker-а {worker_id}')
    release_date(selected_date)
else:
    print('Нет доступных дат для выбора')
"""