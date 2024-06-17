import websocket
import json
import logging
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def on_message(ws, message):
    print("Received Message:", message)

    # Parse the received message as JSON
    data = json.loads(message)
    
    # Extract the symbol and depth data from the message
    symbol = data['s']
    bids = data['b']
    asks = data['a']
    
    # Create a dictionary to store the depth data
    depth_data = {
        'bids': bids,
        'asks': asks
    }
    
    # Store the depth data in Redis
    redis_client.set(symbol, json.dumps(depth_data))
    print(f"Depth data stored in Redis. Key: {symbol}")


def on_error(ws, error):
    print("Error:")
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    params = {
        "method": "SUBSCRIBE",
        "params": [
            "ETH-240518-3000-C@depth10",
            #"ETH-240518-3000-C@ticker",
            #"ETH-240518-2975-C@trade",
            #"BTC-240518-67000-C@bookTicker",
            #"BTCUSDT_180524_66500_call@trade",
            #"BTCUSDT_180524_67000_call@trade"
        ],
        "id": 10
    }
    ws.send(json.dumps(params))

    params = {
        "method": "LIST_SUBSCRIPTIONS",
        "id": 11
    }
    ws.send(json.dumps(params))

def on_ping(ws, message):
    # Send a pong frame with the received ping payload
    ws.send(message, websocket.ABNF.OPCODE_PONG)

if __name__ == "__main__":
    websocket.enableTrace(logging.DEBUG)
    ws = websocket.WebSocketApp("wss://nbstream.binance.com/eoptions/ws",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_ping=on_ping)
    ws.on_open = on_open
    ws.run_forever()
