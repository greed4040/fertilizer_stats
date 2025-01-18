import redis
import json
import utils_workers_helper as uw
import datetime

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def test_call_for_files(selected_date):
    uw.read_date_from_redis_and_generate_filtered_list_around_current_price(selected_date)

def redis_dates():
    return redis_client.get("option_contracts_active").decode("utf-8")

def redis_contracts():
    answer = {}
    dates = redis_dates()
    for el in json.loads(dates):
        contracts = redis_client.get(f"worker:{el}").decode("utf-8")

        contracts_dict = json.loads(contracts)
        #print("supposedly dict:", type(contracts_dict), contracts_dict)
        answer[el] = contracts_dict
    return answer

def get_all_unfiltered_contracts_from_file(redis_date):
    contract_prefix = "ETHUSDT"

    path = "/home/greed/services/trading_algo/binance_websocket_client/" 
    print("#uh redis date:", redis_date, type(redis_date))
    redis_date = int(redis_date)

    contracts = []
    #read ALL contrats from file
    with open(f"{path}/contracts.json", 'r') as file:  
        contracts = json.load(file)['optionSymbols']
    print("###", type(contracts))

    puts=[]
    calls=[]

    for el in contracts:
        #print(type(el))
        #print(el["expiryDate"])
        #print(spot_symbol)
        #print(el)
        #print(type(el['expiryDate']))
        #print(type(redis_date))
        #print()

        dates_match = int(el["expiryDate"]) == int(redis_date)
        #print(el['expiryDate'], dates_match)

        if contract_prefix in el["underlying"] and dates_match:
            debug_string = f"""{datetime.datetime.now()} 
                  {el["expiryDate"]} 
                  {redis_date} 
                  {el["expiryDate"] == redis_date} 
                  {el["underlying"]}
                  {contract_prefix in el["underlying"]}
                  {el["symbol"]}
                  """
            print(debug_string)
            #print(el["expiryDate"] == redis_date, el["expiryDate"], redis_date)
            #print(el,"\n\n\n")
            if el['side']=="PUT": puts.append(el['symbol'])
            if el['side']=="CALL": calls.append(el['symbol'])
        #print(el)
    return {
        'c':sorted(calls), 'p':sorted(puts) 
    }

a=json.loads(redis_dates())[0]
print(a)

ans = get_all_unfiltered_contracts_from_file(a)
print(json.dumps(ans, indent=4))

contracts = [cont for el_type in ans for cont in ans[el_type]]
print("###", contracts)