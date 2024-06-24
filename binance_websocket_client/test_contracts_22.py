import json
import datetime

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

def read_date_from_redis_and_generate_filtered_list_around_current_price(redis_date, contract_prefix = "ETH",  depth=2):
    #Lets filter contracts and get some of them in the money
    #read spot price from file
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
        #print(type(el['expiryDate']))
        #print(type(redis_date))
        #print()

        dates_match = int(el["expiryDate"]) == int(redis_date)
        #print(el['expiryDate'], dates_match)

        if contract_prefix in el["underlying"] and dates_match:
            print(datetime.datetime.now(), 
                  el["expiryDate"], 
                  redis_date, 
                  el["expiryDate"] == redis_date, 
                  el["underlying"], 
                  contract_prefix in el["underlying"]
                  )
        
            #print(el["expiryDate"] == redis_date, el["expiryDate"], redis_date)
            #print(el,"\n\n\n")
            if el['side']=="PUT": puts.append(el)
            if el['side']=="CALL": calls.append(el)
        #print(el)
        
        
        
    print("calls:",  len(calls))
    print("puts:", len(puts))
    calls.sort(key=lambda x: x['strikePrice'], reverse = True)
    puts.sort(key=lambda x: x['strikePrice'], reverse=False)

    calls_answer = []
    call_ctr = 0
    for el in calls:
        print(el['strikePrice'], el["side"])
        if float(el["strikePrice"]) < spot:
            if call_ctr < depth:
                call_ctr += 1
                calls_answer.append(el['symbol'])
                print("CALL added under current price: ", el['symbol'])
        if float(el["strikePrice"]) > spot:
            calls_answer.append(el['symbol'])
            print("CALL added normally: ", el['symbol'])

    puts_answer = []
    puts_ctr = 0
    for el in puts:
        #print(el['strikePrice'], el["side"])
        if float(el["strikePrice"]) > spot:
            if puts_ctr < depth:
                puts_ctr += 1
                puts_answer.append(el['symbol'])
                print("PUT added over current price: ", el['symbol'])
        if float(el["strikePrice"]) < spot:
            puts_answer.append(el['symbol'])
            print("PUT added normally: ", el['symbol'])


    print("\n")

    return { "calls": calls_answer, "puts": puts_answer }

def read_file(selected_date):
    with open(f"contracts_filtered_by_date_in_the_money_{selected_date}.json", 'r') as file:
        contracts = json.load(file)
    return contracts

#all_dates = get_all_expiry_dates()
#print(all_dates)

selected_date = 1719129600000
#c = read_file(selected_date)

c2 = read_date_from_redis_and_generate_filtered_list_around_current_price(selected_date)
print(c2)
