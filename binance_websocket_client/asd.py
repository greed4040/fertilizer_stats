import websocket
import json

def on_message(ws, message):
    print("Received Message:")
    print(json.loads(message))

def on_error(ws, error):
    print("Error:")
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    # Construct the message to subscribe to options prices
    # This example subscribes to BTCUSD options, replace with your specific options symbol if necessary
    params = {
        "method": "SUBSCRIBE",
        "params": [
            "ethusdt@trade"  # Modify this to the specific options trading pair
        ],
        "id": 1
    }
    ws.send(json.dumps(params))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://fstream.binance.com/stream",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()

