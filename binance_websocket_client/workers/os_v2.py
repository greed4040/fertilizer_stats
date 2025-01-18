import websocket
import json
import logging
import redis
import utils_workers_helper as uw

import threading
import time
import datetime
import utils_workers_helper
global selected_date
global request_ctr
global subscribed_buffer
global worker_id
global event_ctr
global thread_started


thread_started = 0
event_ctr = 0
path = "/home/greed/services/trading_algo/binance_websocket_client/worker"


subscribed_buffer = []
request_ctr = 10
selected_date = None
#storage = "test_dates_storage"
storage = "option_contracts_active"

# Set up redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

class MicrosecondFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
            return s.replace('%f', f'{ct.microsecond:06d}')
        else:
            return ct.strftime("%Y-%m-%d %H:%M:%S.%f")

def time_now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

import os
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
def setup_logging(worker_id):
    logger = logging.getLogger(__name__)
    
    # Get log level from environment variable, default to DEBUG
    log_level = os.environ.get('LOG_LEVEL', 'DEBUG').upper()
    logger.setLevel(getattr(logging, log_level, logging.DEBUG))

    # Create formatters and add them to handlers
    formatter = MicrosecondFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f')
    
    # File handler with rotation
    try:
        file_handler = RotatingFileHandler(f"app_worker_{worker_id}.log", maxBytes=10*1024*1024, backupCount=5)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)  # Always log debug and above to file
        logger.addHandler(file_handler)
    except Exception as e:
        print(f"Failed to create file handler: {e}")

    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(getattr(logging, log_level, logging.INFO))  # Use the specified level for console
    logger.addHandler(stream_handler)

    return logger


def websocket_connected(ws):
    try:
        return ws.sock is not None and ws.sock.fileno() != -1
    except AttributeError:
        return False

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
        logger.info("# Sending unsubscription request to API:", params)
        ws.send(json.dumps(params))
        subscribed_buffer.clear()

def subscribe_to_all(ws, contracts):
    global selected_date
    global subscribed_buffer
    global request_ctr
    
    request_ctr += 1
    params = {
        "method": "SUBSCRIBE",
        "params": [f"{contract}@depth10" for contract in contracts],
        "id": request_ctr
    }

    logger.info("(subscribe_to_all) # Sending request to api:", params)
    ws.send(json.dumps(params))
    redis_client.set(f"worker:{selected_date}", json.dumps(contracts))
    subscribed_buffer = contracts

def periodic_check(ws):
    global selected_date
    global logger
    global thread_started
    global worker_id
    c_external = [] # variable which holds list of contracts recieved from external function
    # Check if worker_id is properly initialized
    if 'worker_id' not in globals() or worker_id is None:
        logger.error("(per_check) # worker_id is not properly initialized")
        worker_id = "undefined"  # Set a default value

    thread_started = 1
    logger.info(f"(per_check) # Changing thread count protection parameter: {thread_started}")
    while True:
        logger.info("(per_check) # ---- inner loop ----")
        time.sleep(10)
        logger.info(f"(per_check) #")
        logger.info(f"(per_check) # type ws:{type(ws)}, connected:{websocket_connected(ws)}")
        logger.info(f"(per_check) # thread ID: {threading.get_ident()}")
        logger.info(f"(per_check) # {selected_date}")
        logger.info(f"(per_check) # {subscribed_buffer}")
        if websocket_connected(ws):
            logger.info("(per_check) # Websocket is connected")
            if selected_date:
                try:
                    check = uw.check_date_validity(selected_date, logger)
                    logger.info(f"(per_check) # date: {selected_date} is_date_valid: {check}")

                    if not check:
                        logger.info("(per_check) # Date is not valid, unsubscribing")
                        unsubscribe_from_all(ws)
                        time.sleep(20)
                        selected_date = None
                        logger.info("(per_check) # selected_date released")
                except Exception as e:
                    logger.error(f"(per_check) # Exception while checking date validity: {e}")

            if selected_date == None:
                logger.info("(per_check) # Date is none. Attempting to reserve a new date")
                try:
                    logger.info(f"(per_check) # not yet selected date {selected_date} {worker_id}")
                    selected_date = uw.reserve(storage, worker_id, logger)
                    logger.info(f"(per_check) # new selected date {selected_date} {worker_id}")
                except Exception as e:
                    logger.error(f"(per_check) # Exception while reserving date: {e}")

            if subscribed_buffer == [] and selected_date != None:
                logger.info("(per_check) # Attempting to subscribe to contracts")
                try:
                    #filtered
                    #c_external = uw.read_date_from_redis_and_generate_filtered_list_around_current_price(selected_date)

                    #unfiltered
                    c_external = uw.read_all_unfiltered_contracts_from_file_by_date(selected_date)

                    contracts = [cont for el_type in c_external for cont in c_external[el_type]]

                    subscribe_to_all(ws, contracts)
                    logger.info("(per_check) # Subscribed to contracts")

                except Exception as e:
                    logger.info(f"(per_check) # exc3 {e}")
        
            if subscribed_buffer != [] and selected_date != None:
                logger.info("(per_check) # Checking if subscribed contracts match current list")
                try:
                    c_external = uw.read_all_unfiltered_contracts_from_file_by_date(selected_date)
                    sorted_contracts_in_file=[]
                    for el in c_external:
                        for cont in c_external[el]:
                            sorted_contracts_in_file.append(cont)
                    sorted_contracts_in_file = sorted(sorted_contracts_in_file)
                    sorted_contracts_in_buff = sorted(subscribed_buffer)
                    if sorted_contracts_in_file != sorted_contracts_in_buff:
                        logger.info("(per_check) # List of contracts don't match")
                        logger.info(f"(per_check) # extr: {sorted_contracts_in_file}")
                        logger.info(f"(per_check) # subs: {sorted_contracts_in_buff}")
                        unsubscribe_from_all(ws)
                    else:
                        logger.info("(per_check) # Contract lists match")
                except Exception as e:
                    logger.error(f"(per_check) # Exception while checking contract match: {e}")
        else:
            logger.warning("(per_check) # Websocket not connected")
        pass    
    pass

def on_open(ws):
    logger.debug("(on_open) # opened")
    pass

def on_close(ws):
    logger.debug("(on_close) # connection closed")
    global selected_date
    global subscribed_buffer
    global worker_id

    if selected_date:
        try:
            # Release the date lock
            uw.release_date(selected_date, worker_id, logger)
            logger.info(f"(on_close) # Released date lock for date: {selected_date}")
        except Exception as e:
            logger.error(f"(on_close) # Failed to release date lock: {e}")

    selected_date = None
    subscribed_buffer = []
    logger.info(f"(on_close) # Cleared selected_date and subscribed_buffer: {selected_date}, {subscribed_buffer}")

    # Add a small delay before attempting to reconnect
    time.sleep(5)

    # Attempt to reconnect
    logger.info("(on_close) # Attempting to reconnect: executing start_ws")
    start_ws(worker_id)

def on_error(ws, error):
    logger.error("(on error) # error: {error}")
    pass

def on_ping(ws, message):
    logger.debug(f"ping: {message}")
    ws.send(message, websocket.ABNF.OPCODE_PONG)
    global request_ctr
    request_ctr += 1
    pass

def on_message(ws, message):
    global worker_id
    global event_ctr

    event_ctr += 1
    # Dicecting recieved JSON
    data = json.loads(message)
    #print(data)
    if "result" not in data:
        with open(f"log_messages_result_{worker_id}.log", "a") as outfile:
            outfile.write(f"{time_now()}: {message}"+"\n")

        # Извлечение символа и данных глубины из сообщения
        symbol = data['s']
        bids = data['b']
        asks = data['a']
        
        if bids!=[] or asks!=[]:
            # Создание словаря для хранения данных глубины
            depth_data = {
                'bids': bids,
                'asks': asks,
                't': data['T'],
                'e': data['E']
            }
            # Сохранение данных глубины в Redis
            redis_client.set(symbol, json.dumps(depth_data))
            #print(f"Depth in Redis: {symbol}")

        # Reconnect condition
        #if bids!=[] and asks!=[]:
        #    logger.info("### ###unsubscribe in messages???")
        #    logger.info(message)
        #    params = {
        #        "method": "UNSUBSCRIBE",
        #        "params": [f"{symbol}@depth10"],
        #        "id": 10
        #    }
        #    ws.send(json.dumps(params))
    
    if "result" in data:
        with open(f"log_messages_no_result_{worker_id}.log", "a") as outfile:
            outfile.write(f"{time_now()}: {message}"+"\n")

        if type(data['result'])==list:
            logger.info(type(data['result']))
            logger.info(f"Data: {message}")

            for el in data['result']:
                logger.info(el)
                if el not in subscribed_buffer:
                    subscribed_buffer.append(el.split("@")[0])
                    logger.info("#modifying subscribed buffer: {subscribed_buffer}")

    if event_ctr % 100 == 0:
        logger.debug(f"messages processed: {event_ctr}")

def start_ws(input_worker_id):
    global thread_started
    worker_id = input_worker_id
    if thread_started==0:
        logger = setup_logging(worker_id)
    logger.info("(start_ws) # launching start_ws")

    max_reconnect_attempts = 100
    reconnect_attempts = 0
    reconnect_delay = 5  # Start with 5 seconds delay

    worker_id = input_worker_id


    
    ws = websocket.WebSocketApp("wss://nbstream.binance.com/eoptions/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_ping=on_ping)
    ws.on_open = on_open

    if thread_started == 0:
        logger.info("(start_ws) # Starting a thread with periodic check")
        check_thread = threading.Thread(target=periodic_check, args=(ws,))
        check_thread.daemon = True
        check_thread.start()
        thread_started = 1
        logger.info(f"(start_ws) # Started a thread with periodic check: {thread_started}")

    while True:
        logger.info(f"(start_ws) # connector attempts:{reconnect_attempts} max_attempts:{max_reconnect_attempts}")
        try:
            ws.run_forever()
            logger.info("(start_ws) # WebSocket connection closed normally.")
            reconnect_attempts = 0
        except websocket.WebSocketConnectionClosedException:
            logger.warning("(start_ws) # WebSocket connection closed unexpectedly. Attempting to reconnect...")
        except Exception as e:
            logger.error(f"(start_ws) # Unexpected error occurred: {e}")

        reconnect_attempts += 1
        if reconnect_attempts >= max_reconnect_attempts:
            logger.critical("(start_ws) # Max reconnection attempts reached. Exiting.")
            break

        logger.info(f"(start_ws) # Reconnection attempt {reconnect_attempts} of {max_reconnect_attempts}")
        time.sleep(reconnect_delay)
        reconnect_delay *= 1.2
        logger.info(f"(start_ws) # Timeout delay {reconnect_delay} finished, restarting")

    logger.info("(start_ws) # WebSocket connection handler exiting.")

#if "__main__":
#    start_ws()


