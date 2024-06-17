import websocket
import json
import logging
import redis
import time

# Redis connection setup
redis_client = redis.Redis(host='localhost', port=6379, db=0)
symbol = 'ethusdt'  # The desired symbol here, e.g., 'ethusdt' for ETHUSDT
    
# Message handling
def on_message(ws, message):
    # print("Received Message:", message)
    # Parse the received message as JSON
    data = json.loads(message)
    
    # Save the depth data in Redis
    redis_client.set(symbol, json.dumps(data))
    print(f"Depth in Redis: {symbol} {data}")

# Error handling
def on_error(ws, error):
    print("Error:")
    print(error)

# Connection open handling
def on_open(ws):
    params = {
        "method": "SUBSCRIBE",
        "params": [f"{symbol.lower()}@depth10"],
        "id": 10
    }
    ws.send(json.dumps(params))
    
    params = {
        "method": "LIST_SUBSCRIPTIONS",
        "id": 11
    }
    ws.send(json.dumps(params))

# Ping handling
def on_ping(ws, message):
    # Send a pong frame with the received ping payload
    ws.send(message, websocket.ABNF.OPCODE_PONG)

# Connection close handling
def on_close(ws):
    print("### closed ###")
    print("### restarting ###")
    open_connection()

def open_connection():
    while True:
        ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws",
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    on_ping=on_ping)
        ws.on_open = on_open
        try:
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except KeyboardInterrupt:
            print("### Exiting ###")
            exit()
            
        except Exception as e:
            print(f"Exception occurred: {e}")
            time.sleep(5)  # Wait before trying to reconnect

if __name__ == "__main__":
    websocket.enableTrace(logging.DEBUG)
    open_connection()