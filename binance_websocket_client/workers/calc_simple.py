import redis
import json
from datetime import datetime
import time

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def get_active_dates():
    return json.loads(redis_client.get("option_contracts_active").decode("utf-8"))

def get_contracts_for_date(date):
    contracts = redis_client.get(f"worker:{date}").decode("utf-8")
    return json.loads(contracts)

def get_prices_for_contract(contract):
    price_data = redis_client.get(contract)
    if price_data:
        return json.loads(price_data.decode('utf-8'))
    return None

def process_contracts():
    dates = get_active_dates()
    all_prices = {}

    for date in dates:
        contracts = get_contracts_for_date(date)
        for contract in contracts:
            price_data = get_prices_for_contract(contract)
            if price_data:
                itm_list = contract.split("-")
                best_bid = price_data["bids"][0] if price_data["bids"] else None
                best_ask = price_data["asks"][0] if price_data["asks"] else None
                all_prices[contract] = {
                    "edate": itm_list[1],
                    "strk": itm_list[2],
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "iv": {
                        "iv_bid": 0,
                        "iv_ask": 0
                    }
                }
    return all_prices

def calc_all():
    start_time = datetime.now()
    best_prices = process_contracts()
    elapsed_time = (datetime.now() - start_time).total_seconds()

    response = {
        'delay': elapsed_time,
        'command': 'get_all_prices',
        'best_prices': best_prices,
        'generated': str(datetime.now())
    }

    values = json.dumps(best_prices)
    redis_client.set("calculated_data", values)

while True:
    calc_all()
    print(datetime.now(), "calculated")
    time.sleep(5)

