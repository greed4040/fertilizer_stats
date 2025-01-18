import websocket
import json
import mysql.connector
import time
import datetime

import redis
# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

global ctr
global buffer
global request_ctr
global contract
ctr = 0
request_ctr = 10
buffer = {"b":0, "a":0}
# Set up MySQL connection
db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()
contract = ""

def insert_msg(inst, bid, bid_size, ask, ask_size):
    # Insert data into MySQL
    print("insert_msg called with:", inst, bid, bid_size, ask, ask_size)
    # Insert data into MySQL
    insert_query = f"""
        INSERT INTO {inst[:3]}_prices (datetime, bid, bid_size, ask, ask_size)
        VALUES (NOW(), %s, %s, %s, %s)
        """
    print(insert_query)
    try:
        cursor.execute(insert_query, (bid, bid_size, ask, ask_size))
        conn.commit()
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")

def on_message(ws, message):
    global ctr
    global contract
    global buffer
    ctr += 1
    print(f"Message received: {ctr}")

    data = json.loads(message)
    print("Parsed data:", data)

    if 'bids' in data and 'asks' in data:
        print("Data contains bids and asks")
        bid = float(data['bids'][0][0])
        bid_size = float(data['bids'][0][1])
        ask = float(data['asks'][0][0])
        ask_size = float(data['asks'][0][1])

        if buffer["b"] != bid or buffer["a"] != ask:
            print("Bid or ask changed")
            buffer["b"] = bid
            buffer["a"] = ask
            insert_msg(contract, bid, bid_size, ask, ask_size)
            redis_client.set(f"{contract}_spot_price", json.dumps({"bid": bid, "bid_size": bid_size, "ask": ask, "ask_size": ask_size}))
    
    if ctr < 5 or ctr % 10 == 0:
        print(datetime.datetime.now(), ctr, message)
        print(buffer)
        print("-")


def on_error(ws, error):
    print("Error:")
    print(error)





def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed: {close_status_code} - {close_msg}")

def on_open(ws):
    print("WebSocket connection opened")
    subscribe_to_stream(ws)

def on_ping(ws, message):
    ws.send(message, websocket.ABNF.OPCODE_PONG)
    global request_ctr
    request_ctr += 1

def subscribe_to_stream(ws):
    global cobtract
    params = {
        "method": "SUBSCRIBE",
        "params": [
            f"{contract}@depth5"
        ],
        "id": 1
    }
    ws.send(json.dumps(params))
    print(f"Subscribed to {contract}@depth5 stream")

def run_websocket(input_instrument):
    global contract
    contract = input_instrument
    while True:
        try:
            print("Connecting to WebSocket...")
            ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws",
                                        on_message=on_message,
                                        on_error=on_error,
                                        on_close=on_close,
                                        on_ping=on_ping,
                                        on_open=on_open)
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except Exception as e:
            print(f"WebSocket connection failed: {e}")
        print("Reconnecting in 5 seconds...")
        time.sleep(5)

#if __name__ == "__main__":
#    websocket.enableTrace(True)
#    run_websocket()