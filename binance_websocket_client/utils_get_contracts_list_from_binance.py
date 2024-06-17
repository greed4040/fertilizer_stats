import requests
import json
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def run():
    base_url = 'https://eapi.binance.com'
    endpoint = '/eapi/v1/exchangeInfo'
    response = requests.get(base_url + endpoint)
    data = response.json()
    print("response:",len(data), response)
    with open('/home/greed/services/trading_algo/binance_websocket_client/contracts.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)

def save_current_price(symbol):
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/avgPrice'
    params = {'symbol': symbol}
    
    response = requests.get(base_url + endpoint, params=params)
    data = response.json()

    with open('/home/greed/services/trading_algo/binance_websocket_client/contracts_spot.json', 'w') as outfile:
        json.dump({"contract_name":symbol, "price":data}, outfile, indent=4)

run()
save_current_price("ETHUSDT")

