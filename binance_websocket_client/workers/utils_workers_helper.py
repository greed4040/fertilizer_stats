import redis
import os
import time
import json
import uuid
import datetime
# Подключение к Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Уникальный идентификатор worker-а (можно использовать PID или сгенерировать UUID)
#worker_id = str(uuid.uuid4())
worker_id = 1

def select_date(input_storage, worker_id, logger):
    """
    Выбирает дату из списка доступных дат в Redis и регистрирует её с уникальным идентификатором worker-а.
    """
    dates = json.loads(redis_client.get(input_storage))
    logger.info(dates)
    
    for date in dates:
        # Попытка установить флаг для даты в Redis с уникальным идентификатором worker-а
        if redis_client.setnx(f'worker:date:{date}', f"{worker_id}:{os.getpid()}"):
            # Если дата успешно зарегистрирована, возвращаем её
            return date
    return None

def reserve(input_storage, worker_id, logger):
    while True:
        selected_date = select_date(input_storage, worker_id, logger)
        if selected_date:
            return selected_date
        else:
            msg = f"waiting for a free new date"
            logger.info(msg)
            time.sleep(5)

def release_date(date, worker_id, logger):
    """
    Освобождает дату в Redis, чтобы её мог выбрать другой worker.
    """
    r1=redis_client.delete(f'worker:date:{date}')
    r2=redis_client.delete(f'worker:{date}')
    msg1 = f"deleted lock: {r1}"
    msg2 = f"deleted info: {r2}"
    logger.info(msg1)
    logger.info(msg2)

def check_date_validity(date, logger):
    """
    Проверяет актуальность даты в списке активных контрактов в Redis.
    """
    dates = json.loads(redis_client.get('option_contracts_active'))
    logger.info(f"dates: {dates}; type date[0]:{type(dates[0])}; type date:{type(date)}")
    return date in dates

def read_date_from_redis_and_generate_filtered_list_around_current_price(redis_date, contract_prefix = "ETH",  depth=2):
    path = "/home/greed/services/trading_algo/binance_websocket_client/" 
    #Lets filter contracts and get some of them in the money
    #read spot price from file
    with open(f"{path}/contracts_spot.json", 'r') as file: spot = float(json.load(file)["price"]["price"])
    print("#spot price from file:", spot)
    

    #get expiration date from redis
    #redis_date = int(redis_client.get("option_contracts_date").decode('utf-8'))
    print("#uh redis date:", redis_date, type(redis_date))
    redis_date = int(redis_date)

    #read ALL contrats from file
    with open(f"{path}/contracts.json", 'r') as file:  
        contracts = json.load(file)['optionSymbols']
    print("###", type(contracts))

    calls = []
    puts = []

    for el in contracts:
        #print(type(el))
        #print(el["expiryDate"])
        #print(spot_symbol)
        #print(el)
        #print(type(el['expiryDate']))
        #print(type(redis_date))
        #print()

        dates_match = int(el["expiryDate"]) == int(redis_date)
        #print(el['expiryDate'], dates_match)

        if contract_prefix in el["underlying"] and dates_match:
            print(datetime.datetime.now(), 
                  el["expiryDate"], 
                  redis_date, 
                  el["expiryDate"] == redis_date, 
                  el["underlying"], 
                  contract_prefix in el["underlying"]
                  )
        
            #print(el["expiryDate"] == redis_date, el["expiryDate"], redis_date)
            #print(el,"\n\n\n")
            if el['side']=="PUT": puts.append(el)
            if el['side']=="CALL": calls.append(el)
        #print(el)
        
        
        
    print("calls:",  len(calls))
    print("puts:", len(puts))
    calls.sort(key=lambda x: x['strikePrice'], reverse = True)
    puts.sort(key=lambda x: x['strikePrice'], reverse=False)

    calls_answer = []
    call_ctr = 0
    for el in calls:
        print(el['strikePrice'], el["side"])
        if float(el["strikePrice"]) < spot:
            if call_ctr < depth:
                call_ctr += 1
                calls_answer.append(el['symbol'])
                print("CALL added under current price: ", el['symbol'])
        if float(el["strikePrice"]) > spot:
            calls_answer.append(el['symbol'])
            print("CALL added normally: ", el['symbol'])

    puts_answer = []
    puts_ctr = 0
    for el in puts:
        #print(el['strikePrice'], el["side"])
        if float(el["strikePrice"]) > spot:
            if puts_ctr < depth:
                puts_ctr += 1
                puts_answer.append(el['symbol'])
                print("PUT added over current price: ", el['symbol'])
        if float(el["strikePrice"]) < spot:
            puts_answer.append(el['symbol'])
            print("PUT added normally: ", el['symbol'])


    print("\n")
    return { "calls": calls_answer, "puts": puts_answer }
    #return { "calls": calls, "puts": puts }

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