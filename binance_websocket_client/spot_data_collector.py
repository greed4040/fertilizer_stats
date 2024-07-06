import websocket
import json
import mysql.connector
import time
import datetime

global ctr
global buffer
global request_ctr
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


def insert_msg(bid, bid_size, ask, ask_size):
    # Insert data into MySQL
# Insert data into MySQL
    insert_query = """
        INSERT INTO eth_prices (datetime, bid, bid_size, ask, ask_size)
        VALUES (NOW(), %s, %s, %s, %s)
        """
    try:
        cursor.execute(insert_query, (bid, bid_size, ask, ask_size))
        conn.commit()
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")

def on_message(ws, message):
    global ctr
    ctr += 1
    data = json.loads(message)
    #print(data, type(data), "asks" in data, "bids" in data)
    if 'bids' in data and 'asks' in data:
        bid = data['bids'][0][0]
        bid_size = data['bids'][0][1]
        ask = data['asks'][0][0]
        ask_size = data['asks'][0][1]
        #print(buffer)
        if buffer["b"]!=bid or buffer["a"]!=ask:
            buffer["b"]=bid
            buffer["a"]=ask
            insert_msg(bid, bid_size, ask, ask_size)
            ctr += 1
    if ctr<5 or ctr % 10 == 0:
        print(datetime.datetime.now(), ctr, message)

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
    params = {
        "method": "SUBSCRIBE",
        "params": [
            "ethusdt@depth5"
        ],
        "id": 1
    }
    ws.send(json.dumps(params))
    print("Subscribed to ETH/USDT@depth5 stream")

def run_websocket():
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

if __name__ == "__main__":
    websocket.enableTrace(True)
    run_websocket()