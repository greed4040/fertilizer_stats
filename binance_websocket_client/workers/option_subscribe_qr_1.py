import websocket
import json
import logging
import redis
import utils_workers_helper as uw

import logging
import threading
import time
import datetime
import utils_workers_helper
global selected_date
global request_ctr
global subscribed_buffer
global worker_id

path = "/home/greed/services/trading_algo/binance_websocket_client/worker"
worker_id = 1

subscribed_buffer = []
request_ctr = 10
selected_date = None
storage = "test_dates_storage"
storage = "option_contracts_active"

# Set up redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

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

logger.debug("Отладочное сообщение")
logger.info("Информационное сообщение")
logger.warning("Предупреждающее сообщение")
logger.error("Сообщение об ошибке")
logger.critical("Критическое сообщение")

def websocket_connected(ws):
    return ws.sock is not None and ws.sock.fileno() != -1
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

def subscribe_to_all(ws, contracts):
    global subscribed_buffer
    global request_ctr
    
    request_ctr += 1
    params = {
        "method": "SUBSCRIBE",
        "params": [f"{contract}@depth10" for contract in contracts],
        "id": request_ctr
    }

    print("# Sending request to api:", params)
    ws.send(json.dumps(params))
    subscribed_buffer = contracts

def periodic_check(ws):
    global selected_date
    while True:
        time.sleep(10)
        print(f"{datetime.datetime.now()} periodick check")
        if websocket_connected(ws):
            if selected_date:
                check = uw.check_date_validity(selected_date)
                print(datetime.datetime.now(), "date:", selected_date, "check:", check)

                if check == False:
                    unsubscribe_from_all(ws)
                    time.sleep(20)
                    selected_date = None
                    print("selected_date released")

            if selected_date == None:
                print("new selected date", selected_date)
                selected_date = uw.reserve(storage, worker_id)
                print("new selected date", selected_date)

            if subscribed_buffer == [] and selected_date != None:
                c = uw.read_date_from_redis_and_generate_filtered_list_around_current_price(selected_date)
                contracts = []
                for el_type in c:
                    for cont in c[el_type]:
                        contracts.append(cont)

                subscribe_to_all(ws, contracts)
                print("subscribed")
        pass    
    pass

def on_open(ws):
    print(dir(ws))

    logger.debug("opened")

    pass

def on_close(ws):
    logger.debug("closed")
    pass

def on_error(ws, error):
    logger.error(error)
    pass

def on_ping(ws, message):
    logger.debug("ping")
    ws.send(message, websocket.ABNF.OPCODE_PONG)
    global request_ctr
    request_ctr += 1
    pass

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
       

def start_ws():
    #websocket.enableTrace(logging.DEBUG)
    ws = websocket.WebSocketApp("wss://nbstream.binance.com/eoptions/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_ping=on_ping
                                )
    ws.on_open = on_open

    # Start the periodic check in a separate thread
    check_thread = threading.Thread(target=periodic_check, args=(ws,))
    check_thread.daemon = True
    check_thread.start()

    ws.run_forever()

if "__main__":
    start_ws()


