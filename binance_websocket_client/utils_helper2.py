import requests
import json
import redis
import datetime

redis_client = redis.Redis(host='localhost', port=6379, db=0)

def request_current_avg_spot_price(symbol):
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/avgPrice'
    params = {'symbol': symbol}
    
    response = requests.get(base_url + endpoint, params=params)
    data = response.json()

    return float(data['price'])

def request_option_contracts_from_binance():
    base_url = 'https://eapi.binance.com'
    endpoint = '/eapi/v1/exchangeInfo'
    response = requests.get(base_url + endpoint)
    data = response.json()
    print("response:",len(data), response)
    with open('/home/greed/services/trading_algo/binance_websocket_client/contracts.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)

def get_all_expiry_dates():
    with open("contracts.json", 'r') as file: 
        data = json.load(file)
    contracts = data['optionSymbols']

    exp_lst = []
    for el in contracts:
        if el['expiryDate'] not in exp_lst:
            exp_lst.append(el['expiryDate'])
    print("sorted:", exp_lst.sort())
    return exp_lst

def get_min_expiry_date():
    min_date = get_all_expiry_dates()[0]
    #print(f"min: {min_date}")
    return min_date

def get_current_expiry_date_from_redis():
    response = redis_client.get("option_contracts_date").decode('utf-8')
    return response

def set_min_expiry_date_to_redis():
    #all_dates = get_all_expiry_dates()
    #for rec in all_dates: print("##:",rec)

    min_date = get_min_expiry_date()
    #print(f"min_date: {min_date}")
    redis_client.set("option_contracts_date", min_date)
    updated = redis_client.get("option_contracts_date").decode('utf-8')
    print(updated)

"""
        {
            "contractId": 3,
            "expiryDate": 1719561600000,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "minPrice": "2473.1",
                    "maxPrice": "3003.3",
                    "tickSize": "0.1"
                },
                {
                    "filterType": "LOT_SIZE",
                    "minQty": "0.01",
                    "maxQty": "2500",
                    "stepSize": "0.01"
                }
            ],
            "id": 12020,
            "symbol": "ETH-240628-800-C",
            "side": "CALL",
            "strikePrice": "800.00000000",
            "underlying": "ETHUSDT",
            "unit": 1,
            "makerFeeRate": "0.00030000",
            "takerFeeRate": "0.00030000",
            "minQty": "0.01",
            "maxQty": "2500",
            "initialMargin": "0.15000000",
            "maintenanceMargin": "0.07500000",
            "minInitialMargin": "0.10000000",
            "minMaintenanceMargin": "0.05000000",
            "priceScale": 1,
            "quantityScale": 2,
            "quoteAsset": "USDT"
        },
"""
        
def get_contracts_by_expiry_date_and_prefix(expdate, symbol_prefix):
    ###
    # Read file by filename
    ###
    with open("contracts.json", 'r') as file: 
        data = json.load(file)['optionSymbols']

    answer = []
    # loop thorough countrats in a file recieved from binance
    for el in data:
        if str(el['expiryDate']) == str(expdate) and symbol_prefix in el['symbol']:
            # data to extract from option contract record
            contract_dict = {
                'symbol': el['symbol'],
                'ex_date': el['expiryDate'],
                'side': el['side'],
                'strike': float(el['strikePrice']),
                'id': el['id'],
                'contract_id':el['contractId']
            }
            answer.append(contract_dict)
    return answer

def get_all_redis_date_contracts_from_file(redis_date, contract_prefix):
    #redis_date = redis_client.get("option_contracts_date").decode('utf-8')
    print("#uh get_all_redis_date_contracts_from_file contracts date:", redis_date)
    with open("contracts.json", 'r') as file: 
        contracts = json.load(file)['optionSymbols']

    answer = []
    for el in contracts:
        if str(el['expiryDate']) == redis_date:
            if contract_prefix in el['symbol']:
                answer.append(el['symbol'])
    return answer

def get_all_filtered_read_file_only():
    with open("contracts_filtered_by_date_in_the_money.json", 'r') as file:
        contracts = json.load(file)
    
    return contracts

def read_date_from_redis_generate_filtered_list_around_current_price(redis_date, depth=2):
    #Lets filter contracts and get some of them in the money
    #read spot qts from file
    spot_symbol = "ETHUSDT"
    with open("contracts_spot.json", 'r') as file: spot = float(json.load(file)["price"]["price"])
    print("#spot price from file:", spot)
    
    #get expiration date from redis
    #redis_date = int(redis_client.get("option_contracts_date").decode('utf-8'))
    print("#uh redis date:", redis_date, type(redis_date))
    redis_date = int(redis_date)
    #read ALL contrats from file
    with open("contracts.json", 'r') as file:  
        contracts = json.load(file)['optionSymbols']
    print("###", type(contracts))

    calls = []
    puts = []

    for el in contracts:
        #print(type(el))
        #print(el["expiryDate"])
        #print(spot_symbol)
        #print(el)
        if el["underlying"]== spot_symbol and el["expiryDate"] == redis_date:
            print(datetime.datetime.now(), 
                  el["expiryDate"], 
                  redis_date, 
                  el["expiryDate"] == redis_date, 
                  el["underlying"], 
                  spot_symbol== el["underlying"]
                  )
        
            #print(el["expiryDate"] == redis_date, el["expiryDate"], redis_date)
            #print(el,"\n\n\n")
            if el['side']=="PUT": puts.append(el)
            if el['side']=="CALL": calls.append(el)
        #print(el)
        
        
        
    print("calls:", calls, len(calls))
    print("puts:", puts, len(puts))
    calls.sort(key=lambda x: x['strikePrice'], reverse = True)
    puts.sort(key=lambda x: x['strikePrice'], reverse=False)

    calls_answer = []
    call_ctr = 0
    for el in calls:
        #print(el['strikePrice'], el["side"])
        if float(el["strikePrice"]) < spot:
            if call_ctr < depth:
                call_ctr += 1
                calls_answer.append(el)
        if float(el["strikePrice"]) > spot:
            calls_answer.append(el)

    puts_answer = []
    puts_ctr = 0
    for el in puts:
        #print(el['strikePrice'], el["side"])
        if float(el["strikePrice"]) > spot:
            if puts_ctr < depth:
                puts_ctr += 1
                puts_answer.append(el)
        if float(el["strikePrice"]) < spot:
            puts_answer.append(el)


    print("\n")

    with open("contracts_filtered_by_date_in_the_money.json", "w") as outfile:
        json.dump({ "calls": calls_answer, "puts": puts_answer }, outfile, indent=4)
    
    return { "calls": calls_answer, "puts": puts_answer }


