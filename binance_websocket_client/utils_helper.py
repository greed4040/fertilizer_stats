import json
import requests
import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def get_avg_price(symbol):
    base_url = 'https://api.binance.com'
    endpoint = '/api/v3/avgPrice'
    params = {'symbol': symbol}
    
    response = requests.get(base_url + endpoint, params=params)
    data = response.json()

    return float(data['price'])

def get_fully_updated_contracts_from_api():
    base_url = 'https://eapi.binance.com'
    endpoint = '/eapi/v1/exchangeInfo'
    response = requests.get(base_url + endpoint)
    data = response.json()

    with open('contracts.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)

def pretty_print_dictionary(d):
    # Convert a dictionary to a string JSON with indents
    formatted_json = json.dumps(d, indent=4, ensure_ascii=False)  # Specify the offset indent
    # Produce a formatter JSON
    print(formatted_json)

def read_json(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    # Extracting data
    return data

def get_expiry_dates(filename):
    contracts = read_json(filename)['optionSymbols']

    exp_lst = []
    for el in contracts:
        if el['expiryDate'] not in exp_lst:
            exp_lst.append(el['expiryDate'])

    return sorted(exp_lst)

def get_contracts_by_expiry_prefix_and_date(expdate, symbol_prefix, filename):
    ###
    # Read file by filename
    ###
    contracts = read_json(filename)['optionSymbols']

    #print("# get_contracts_by_expiry_prefix_and_date:", len(contracts), expdate, symbol_prefix)
    answer = []
    for el in contracts:
        #if symbol_prefix in el['symbol']: print(el)
        if el['expiryDate'] == expdate and symbol_prefix in el['symbol']:
            contract_dict = {
                'symbol': el['symbol'],
                'ex_date': el['expiryDate'],
                'side': el['side'],
                'strike': float(el['strikePrice']),
                'id': el['id'],
                'contract_id':el['contractId']
            }
            answer.append(contract_dict)
    #print(len(answer))
    return answer

def get_outOfTheMoney_strikes_puts_calls_list(contracts, current_price, limit):
    calls = []
    puts = []
    for el in contracts:
        if el['side']=='CALL':
            calls.append(el)
        if el['side']=='PUT':
            puts.append(el)
    
    calls.sort(key=lambda x: x['strike'])
    puts.sort(key=lambda x: x['strike'], reverse=True)

    #pretty_print_dictionary(calls)
    print("="*40)
    #pretty_print_dictionary(puts)
    filtered_calls = []
    for el in calls:
        if el['strike']>current_price:
            if len(filtered_calls)<limit: filtered_calls.append(el)

    filtered_puts = []
    for el in puts:
        if el['strike']<current_price:
            if len(filtered_puts)<limit: filtered_puts.append(el)

    #print(len(filtered_calls), filtered_calls)
    #print(len(filtered_puts), filtered_puts)
    return {'puts':filtered_puts, 'calls':filtered_calls}
#get_contracts_from_api()

def get_outOfTheMoney_strikes_puts_calls_list_V2(contracts, current_price, limit):
    #create arrays for instruments
    calls, puts = [], []
    for el in contracts:
        if el['side']=='CALL': calls.append(el)
        if el['side']=='PUT': puts.append(el)
    
    calls.sort(key=lambda x: x['strike'])
    puts.sort(key=lambda x: x['strike'], reverse=True)

    #pretty_print_dictionary(calls)
    print("="*40)
    #pretty_print_dictionary(puts)
    filtered_calls = []
    in_the_money_call_element = {}
    for el in calls:
        #print(el)
        if el['strike'] > current_price:
            if len(filtered_calls) < limit: filtered_calls.append(el)
    for el in calls:
        print(el)
        current_index = calls.index(el)
        if el not in filtered_calls and calls[current_index+1] in filtered_calls:
            in_the_money_call_element = calls[current_index]
            break
    filtered_calls.insert(0,in_the_money_call_element)
    print("\nfiltered_calls:\n")
    for el in filtered_calls:
        print(el)
    print("\n")

    filtered_puts = []
    in_the_money_put_element = {}
    for el in puts:
        if el['strike'] < current_price:
            if len(filtered_puts) < limit: filtered_puts.append(el)
    for el in puts:
        current_index = puts.index(el)
        if el not in filtered_puts and puts[current_index+1] in filtered_puts:
            in_the_money_put_element=el
            break
    filtered_puts.insert(0, in_the_money_put_element)
    print("\nfiltered_puts:\n")
    for el in filtered_puts:
        print(el)
    print("\n")

    #print(len(filtered_calls), filtered_calls)
    #print(len(filtered_puts), filtered_puts)
    return {'puts':filtered_puts, 'calls':filtered_calls}


def extract_filtered_contracts_by_expdate_into_file(expdate, spot_contract, spot_prefix, limit):
    #expiry_list = get_expiry_dates("contracts.json")
    #c = get_contracts_by_expiry_and_date(expdate[0], "ETH", "contracts.json")
    c = get_contracts_by_expiry_prefix_and_date(int(expdate), spot_prefix, "contracts.json")
    #print(c)
    #pretty_print_dictionary(c)
    avg_prc = get_avg_price(spot_contract)
    print(avg_prc)
    organized = get_outOfTheMoney_strikes_puts_calls_list_V2(c, avg_prc, limit)
    print(f"p:{len(organized['puts'])}, c:{len(organized['calls'])}")
    print("# contract details:", spot_contract, spot_prefix)
    with open('contracts_filtered_by_date.json', 'w') as outfile:
        json.dump(organized, outfile, indent=4)
    return "ok"

def get_all_current_date_contracts_only():
    date = redis_client.get("option_contracts_date").decode('utf-8')
    print("contracts date:", date)
    contracts = read_json("contracts.json")['optionSymbols']

    answer = []
    for el in contracts:
        if str(el['expiryDate'])==date:
            #print(el)
            if "ETH" in el['symbol']:
                answer.append(el['symbol'])

    return answer

#spot_contract="ETHUSDT"
#extract_filtered_contracts_by_expdate_into_file(1716537600000, "ETHUSDT", "ETH")
#a=get_contracts_by_expiry_prefix_and_date(1716537600000, "ETH", "contracts.json")
#get_organised_contracts_list(a, get_avg_price(spot_contract))
#extract_filtered_contracts_by_expdate_into_file(1716537600000, "ETHUSDT", "ETH", 5)