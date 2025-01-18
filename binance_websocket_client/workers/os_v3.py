import websocket
import json
import logging
import redis
import utils_workers_helper as uw

import threading
import time
import datetime
import sys
from logging.handlers import RotatingFileHandler
import os

# Global variables
event_ctr = 0
request_ctr = 10
subscribed_buffer = []
worker_id = None
selected_date = None
logger = None
is_initialised = 0 

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

def get_current_date(local_worker_id):
    record_id = f"record_id_{local_worker_id}"
    selected_date_local = redis_client.get(record_id).decode("utf-8")
    return selected_date_local

def websocket_connected(ws):
    try:
        return ws.sock is not None and ws.sock.fileno() != -1
    except AttributeError:
        return False

def unsubscribe_from_all(ws):
    global request_ctr

    if subscribed_buffer:
        request_ctr += 1
        params = {
            "method": "UNSUBSCRIBE",
            "params": [f"{contract}@depth10" for contract in subscribed_buffer],
            "id": request_ctr
        }
        logger.info("# Sending unsubscription request to API:", params)
        ws.send(json.dumps(params))
        subscribed_buffer.clear()

def subscribe_to_all(ws, contracts, worker_id, local_selected_date):
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
    redis_client.set(f"worker:{worker_id}", json.dumps({"c":contracts, "d":local_selected_date}))
    subscribed_buffer = contracts

def manage_subscription(ws, new_selected_date, worker_id):
    global selected_date
    try:
        unsubscribe_from_all(ws)
        c_external = uw.read_all_unfiltered_contracts_from_file_by_date(new_selected_date)
        contracts = [cont for el_type in c_external for cont in c_external[el_type]]
        selected_date = new_selected_date
        subscribe_to_all(ws, contracts, worker_id, selected_date)
        logger.info(f"(manage_subscription) # Subscribed to new contracts: {contracts} {selected_date}")
    except Exception as e:
        logger.error(f"(manage_subscription) # Exception while subscribing to new contracts: {e}")

def periodic_check(ws):
    global logger
    global worker_id
    global selected_date

    while True:
        time.sleep(10)
        if websocket_connected(ws):
            try:
                redis_date = get_current_date(worker_id)
                if redis_date:
                    logger.info(f"(per_check) # Redis value: {redis_date}")
                    if selected_date != redis_date:
                        logger.info(f"(per_check) # Dates do not match. rd:{redis_date}. sel_date:{selected_date}. Unsubscribing and resubscribing.")
                        manage_subscription(ws, redis_date, worker_id)
                    else:
                        logger.info(f"(per_check) # Dates match, both equal to:{redis_date} {event_ctr} {websocket_connected(ws)}")
                else:
                    logger.warning(f"(per_check) # No value found in Redis for {redis_date}")
            except Exception as e:
                logger.error(f"(per_check) # Exception while checking Redis value: {e}")
        else:
            logger.warning("(per_check) # Websocket not connected")

def on_open(ws):
    logger.debug("(on_open) # opened")

def on_close(ws):
    global subscribed_buffer
    global worker_id

    subscribed_buffer = []
    logger.info(f"(on_close) # Cleared subscribed_buffer: {subscribed_buffer}")

    time.sleep(5)
    logger.info("(on_close) # Attempting to reconnect: executing start_ws")
    start_ws(worker_id)

def on_error(ws, error):
    logger.error(f"(on error) # error: {error}")

def on_ping(ws, message):
    logger.debug(f"ping: {message}")
    ws.send(message, websocket.ABNF.OPCODE_PONG)
    global request_ctr
    request_ctr += 1

def on_message(ws, message):
    global worker_id
    global event_ctr

    event_ctr += 1
    data = json.loads(message)
    if "result" not in data:
        with open(f"log_messages_result_{worker_id}.log", "a") as outfile:
            outfile.write(f"{time_now()}: {message}\n")

        symbol = data['s']
        bids = data['b']
        asks = data['a']
        
        if bids or asks:
            depth_data = {
                'bids': bids,
                'asks': asks,
                't': data['T'],
                'e': data['E']
            }
            redis_client.set(symbol, json.dumps(depth_data))
    
    if "result" in data:
        with open(f"log_messages_no_result_{worker_id}.log", "a") as outfile:
            outfile.write(f"{time_now()}: {message}\n")

        if isinstance(data['result'], list):
            for el in data['result']:
                if el not in subscribed_buffer:
                    subscribed_buffer.append(el.split("@")[0])
                    logger.info(f"#modifying subscribed buffer: {subscribed_buffer}")

    if event_ctr % 100 == 0:
        logger.debug(f"messages processed: {event_ctr}")

def start_ws(input_worker_id):
    global worker_id
    global selected_date
    global logger
    global is_initialised

    worker_id = input_worker_id

    if logger is None:
        logger = setup_logging(worker_id)
        logger.info("logger initialised")

    logger.info(f"(start_ws) # Initial selected date: {selected_date}")
    logger.info(f"===================================================")
    
    #logger.info(f"ws connection {websocket_connected(ws)}")
    ws = None
    ws = websocket.WebSocketApp(
        "wss://nbstream.binance.com/eoptions/ws",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_ping=on_ping,
        on_open=on_open
    )
    logger.info(f"ws connection {websocket_connected(ws)}")

    if is_initialised == 0:
        check_thread = threading.Thread(target=periodic_check, args=(ws,))
        check_thread.daemon = True
        check_thread.start()
        is_initialised = 1

    while True:
        try:
            ws.run_forever()
            reconnect_attempts = 0
        except websocket.WebSocketConnectionClosedException:
            logger.warning("(start_ws) # WebSocket connection closed unexpectedly. Attempting to reconnect...")
        except Exception as e:
            logger.error(f"(start_ws) # Unexpected error occurred: {e}")

        reconnect_attempts += 1
        if reconnect_attempts >= 100:
            logger.critical("(start_ws) # Max reconnection attempts reached. Exiting.")
            break

        time.sleep(reconnect_attempts * 5)
        reconnect_attempts *= 1.2

# end of code