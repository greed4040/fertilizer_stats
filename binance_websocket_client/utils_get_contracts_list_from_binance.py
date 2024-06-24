import requests
import json
import redis
#import utils_helper2 as uh2
import time
import datetime

storage = "option_contract_dates" 
path = '/home/greed/services/trading_algo/binance_websocket_client'

def download_contracts():
    global path
    base_url = 'https://eapi.binance.com'
    endpoint = '/eapi/v1/exchangeInfo'
    response = requests.get(base_url + endpoint)
    data = response.json()
    print("response:",len(data), response)
    with open(f"{path}/contracts.json", 'w') as outfile:
        json.dump(data, outfile, indent=4)

def save_current_price(symbol):
    global path
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/avgPrice'
    params = {'symbol': symbol}
    
    response = requests.get(base_url + endpoint, params=params)
    data = response.json()

    with open(f"{path}/contracts_spot.json", "w") as outfile:
        json.dump({"contract_name":symbol, "price":data}, outfile, indent=4)
    
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    redis_client.set(f"contracts_spot_{symbol}", json.dumps(data))

def extract_contracts(contract_prefix):
    def get_all_expiry_dates(contract_prefix):
        global path
        with open(f"{path}/contracts.json", 'r') as file: 
            data = json.load(file)
        contracts = data['optionSymbols']

        exp_lst = []
        for el in contracts:
            if el['expiryDate'] not in exp_lst and contract_prefix  in el['symbol']:
                exp_lst.append(el['expiryDate'])
        print("sorted:", exp_lst.sort())
        return exp_lst
    def get_contracts_by_expiry_date_and_prefix_name_only(input_date, contract_prefix):
        global path
        #redis_date = redis_client.get("option_contracts_date").decode('utf-8')
        print("\n\n#uh get_all_input_date_contracts_from_file contracts date:", input_date, type(input_date))
        with open(f"{path}/contracts.json", 'r') as file: 
            contracts = json.load(file)['optionSymbols']

        answer = []
        for el in contracts:
            if el['expiryDate'] == input_date:
                #print(type(el['expiryDate']), input_date, el['expiryDate'], input_date == el['expiryDate'], el['symbol'], contract_prefix, contract_prefix in el['symbol'])
            
                if contract_prefix in el['symbol']:
                    answer.append(el['symbol'])
                    #print(el['symbol'])
        return answer
    global path
    dates = get_all_expiry_dates(contract_prefix)
    dates_active = []
    for i in range(0,4):
        c = get_contracts_by_expiry_date_and_prefix_name_only(dates[i], contract_prefix)
        print("="*20)
        print(c)
        calls, puts = [], []
        for el in c:
            if "-C" in el: calls.append(el)
            if "-P" in el: puts.append(el)
        

        with open(f"{path}/contracts_filtered_by_date_in_the_money_{dates[i]}.json", "w") as outfile:
            json.dump({ "calls": calls, "puts": puts }, outfile, indent=4)
        dates_active.append(dates[i])

    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    print(dates_active)
    redis_client.set("option_contracts_active", json.dumps(dates_active))

def run():
    #download_contracts()
    download_contracts()

    save_current_price("ETHUSDT")

    contract_prefix="ETH"
    extract_contracts(contract_prefix)

run()
