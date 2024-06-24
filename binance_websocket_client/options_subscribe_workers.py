import utils_workers_helper as uw
import time
import datetime
worker_id = 0
selected_date = None

while True:
    while not selected_date:
        selected_date = uw.select_date(worker_id)
        if not selected_date:
            time.sleep(1)  # Задержка перед повторной попыткой

    if selected_date:
        print(f'Worker {worker_id} выбрал дату {selected_date}')
        # Ваша основная логика здесь
        while uw.check_date_validity(selected_date):
            print(f'{datetime.datetime.now()} Дата {selected_date} всё ещё актуальна для worker-а {worker_id}')
            time.sleep(10)  # Проверяем каждые 10 секунд
        print(f'{datetime.datetime.now()} Дата {selected_date} больше не актуальна для worker-а {worker_id}')
        uw.release_date(selected_date, worker_id)
        selected_date = None
    else:
        print('Нет доступных дат для выбора')
        time.sleep(1)