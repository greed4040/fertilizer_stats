import websocket
import json
import logging
import redis

import logging
import utils_helper2 as uh2
import threading
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG,  # Set the logging level
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Set the log message format
                    datefmt='%Y-%m-%d %H:%M:%S',  # Set the date and time format
                    handlers=[
                        logging.FileHandler("app.log"),  # Log messages to a file
                        logging.StreamHandler()  # Log messages to the console
                    ])

# Create a logger
logger = logging.getLogger(__name__)

# Example usage of the logger
#logger.debug("Debug message")
#logger.info("Info message")
#logger.warning("Warning message")
#logger.error("Error message")
#logger.critical("Critical message")

# Set up redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

subscribed_buffer = []

# Message handling
def on_message(ws, message):
    #print("Received Message:", message)

    # Разбор полученного сообщения как JSON
    data = json.loads(message)
    
    if "result" not in data:
        # Извлечение символа и данных глубины из сообщения
        symbol = data['s']
        bids = data['b']
        asks = data['a']
        
        if bids!=[] and asks!=[]:
            # Создание словаря для хранения данных глубины
            depth_data = {
                'bids': bids,
                'asks': asks
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

# Conn opened event
def on_open(ws):
    # Let's subscribe 
    subscribe_to_contracts(ws)

# Subscribe to all
def subscribe_to_contracts(ws):
    # Read contracts from file
    contracts = uh2.get_contracts_only()
    params = {
        "method": "SUBSCRIBE",
        "params": [f"{contract}@depth10" for contract in contracts],
        "id": 10
    }
    time.sleep(5)
    print("# Sending request to api:", params)
    ws.send(json.dumps(params))

    params = {
        "method": "LIST_SUBSCRIPTIONS",
        "id": 11
    }
    ws.send(json.dumps(params))

# Обработка пинга
def on_ping(ws, message):
    # Отправка pong-фрейма с полученным пинг-пayload
    ws.send(message, websocket.ABNF.OPCODE_PONG)

# Unsubscribe from elements in subscribed_buffer
def unsubscribe_from_all(ws):
    if subscribed_buffer:
        params = {
            "method": "UNSUBSCRIBE",
            "params": [f"{contract}@depth10" for contract in subscribed_buffer],
            "id": 10
        }
        print("# Sending unsubscription request to API:", params)
        ws.send(json.dumps(params))
        subscribed_buffer.clear()

# Check if subscribed contracts match with config
def periodic_check(ws):
    print("lets start checks")
    while True:
        current_contracts = uh.get_contracts_only()
        if set(current_contracts) != set(subscribed_buffer):
            unsubscribe_from_all(ws)
            time.sleep(5)
            subscribe_to_contracts(ws)
            print("periodical check: modifying subscription")
        else:
            print("periodical check: we're all set")
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

    

