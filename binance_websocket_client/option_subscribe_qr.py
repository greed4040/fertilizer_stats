import websocket
import json
import logging
import redis
import utils_workers_helper as uw

import logging
import threading
import time
import datetime

global selected_date
global request_ctr
global subscribed_buffer
global worker_id

worker_id = 0

subscribed_buffer = []
request_ctr = 10
selected_date = None

# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Set the logging level
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log message format
                    datefmt='%Y-%m-%d %H:%M:%S',  # Set the date and time format
                    handlers=[
                        logging.FileHandler(f"app_worker_{worker_id}.log"),  # Log messages to a file
                        logging.StreamHandler()  # Log messages to the console
                    ])

# Create a logger
logger = logging.getLogger(__name__)

# Set up redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)


def read_date_from_redis_and_generate_filtered_list_around_current_price(redis_date, contract_prefix = "ETH",  depth=2):
    logger.info("executing dates parser")
    #Lets filter contracts and get some of them in the money
    #read spot price from file
    with open("contracts_spot.json", 'r') as file: spot = float(json.load(file)["price"]["price"])
    print("#spot price from file:", spot)
    

    #get expiration date from redis
    #redis_date = int(redis_client.get("option_contracts_date").decode('utf-8'))
    print("#uh redis date:", redis_date, type(redis_date))
    redis_date = int(redis_date)

    #read ALL contrats from file
    with open("contracts.json", 'r') as file:  
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
    logger.info("finished executing dates parser")
    return { "calls": calls_answer, "puts": puts_answer }

# Message handling
def on_message(ws, message):
    #print("Received Message:", message)

    # Разбор полученного сообщения как JSON
    data = json.loads(message)
    #print(data)
    if "result" not in data:
        # Извлечение символа и данных глубины из сообщения
        symbol = data['s']
        bids = data['b']
        asks = data['a']
        
        if bids!=[] and asks!=[]:
            # Создание словаря для хранения данных глубины
            depth_data = {
                'bids': bids,
                'asks': asks,
                't': data['T'],
                'e': data['E']
            }
            # Сохранение данных глубины в Redis
            redis_client.set(symbol, json.dumps(depth_data))
            print(f"Depth in Redis: {symbol}")
        else:
            params = {
                "method": "UNSUBSCRIBE",
                "params": [f"{symbol}@depth10"],
                "id": 10
            }
            ws.send(json.dumps(params))
    if "result" in data:
        if type(data['result'])==list:
            logger.info(type(data['result']))
            logger.info(f"Data: {message}")

            for el in data['result']:
                print(el)
                if el not in subscribed_buffer:
                    subscribed_buffer.append(el.split("@")[0])
                    print(subscribed_buffer)
            
# Error handling
def on_error(ws, error):
    print("Error:")
    print(error)

# Conn closed event
def on_close(ws):
    print("### closed ###")

# Subscribe to all
def subscribe_to_contracts_from_file(ws):
    global selected_date
    
    # Let's acquire the date
    selected_date = acquire_date_to_work_with()
    logger.info(f"subscribing to {selected_date}")

    c = read_date_from_redis_and_generate_filtered_list_around_current_price(selected_date)
    contracts = []
    for el_type in c:
        for cont in c[el_type]:
            contracts.append(cont)

    logger.info(f"subscribing to {contracts}")

    params = {
        "method": "SUBSCRIBE",
        "params": [f"{contract}@depth10" for contract in contracts],
        "id": request_ctr
    }

    print("# Sending request to api:", params)
    ws.send(json.dumps(params))

    pass

# Conn opened event
def on_open(ws):
    # Let's subscribe 
    subscribe_to_contracts_from_file(ws)
    """params = {
        "method": "SUBSCRIBE",
        "params": [f"{contract}@depth10" for contract in ['ETH-240621-3900-C']],
        "id": request_ctr
    }

    print("# Sending request to api:", params)
    ws.send(json.dumps(params))
    """
    print("opened")


# Обработка пинга
def on_ping(ws, message):
    # Отправка pong-фрейма с полученным пинг-пayload
    ws.send(message, websocket.ABNF.OPCODE_PONG)

# Unsubscribe from elements in subscribed_buffer
def unsubscribe_from_all(ws):
    global request_ctr

    request_ctr += 1
    if subscribed_buffer:
        params = {
            "method": "UNSUBSCRIBE",
            "params": [f"{contract}@depth10" for contract in subscribed_buffer],
            "id": request_ctr
        }
        print("# Sending unsubscription request to API:", params)
        ws.send(json.dumps(params))
        subscribed_buffer.clear()

def acquire_date_to_work_with():
    local_selected_date = None
    while not local_selected_date:
        local_selected_date = uw.select_date(worker_id)
        if not local_selected_date:
            time.sleep(1)  # Задержка перед повторной попыткой
    return local_selected_date

# Check if subscribed contracts match with config
def periodic_check(ws):
    print("lets start checks")
    while True:
        #current_contracts = uh2.get_contracts_only()
        global selected_date
        unsubscribe_condition = False

        if selected_date:
            print(f'Worker {worker_id} выбрал дату {selected_date}')
            # Ваша основная логика здесь
            while uw.check_date_validity(selected_date):
                print(f'{datetime.datetime.now()} ++ ++ ++ Дата {selected_date} актуальна для worker-а {worker_id}')
                time.sleep(10)  # Проверяем каждые 10 секунд
                logger.info(f"++ ++ ++{selected_date} {worker_id}")
            print(f'{datetime.datetime.now()} -- -- -- Дата {selected_date} больше не актуальна для worker-а {worker_id}')
            logger.info(f"-- -- --{selected_date} {worker_id}")
            uw.release_date(selected_date, worker_id)
            selected_date = None
            unsubscribe_condition = True
        else:
            print("no dates available")
            logger.info(f"-- -- -- no dates available")
            time.sleep(1)

        if unsubscribe_condition:
            logger.info(f"unsubscribing: {unsubscribe_condition}")
            unsubscribe_from_all(ws)
            time.sleep(5)
            
            subscribe_to_contracts_from_file(ws)
            print("periodical check: modifying subscription")
            logger.info("periodical check: modifying subscription")
        else:
            print("periodical check: modifying subscription")
            logger.info("periodical check: modifying subscription")
        time.sleep(5)  # Wait for 1 minute before the next check


def open_connection():
    while True:
        websocket.enableTrace(logging.DEBUG)
        ws = websocket.WebSocketApp("wss://nbstream.binance.com/eoptions/ws",
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_ping=on_ping)
        ws.on_open = on_open

        check_thread = threading.Thread(target=periodic_check, args=(ws,))
        check_thread.daemon = True
        check_thread.start()

        try:
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except KeyboardInterrupt:
            print("### Exiting ###")
            exit()
            
        except Exception as e:
            print(f"Exception occurred: {e}")
            time.sleep(5)  # Wait before trying to reconnect

if "__main__":
    open_connection()

if False == "__main__":
    websocket.enableTrace(logging.DEBUG)
    ws = websocket.WebSocketApp("wss://nbstream.binance.com/eoptions/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_ping=on_ping)
    ws.on_open = on_open

    # Start the periodic check in a separate thread
    check_thread = threading.Thread(target=periodic_check, args=(ws,))
    check_thread.daemon = True
    check_thread.start()

    ws.run_forever()
