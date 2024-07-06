import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from scipy.optimize import newton

import redis
import json
from datetime import datetime
import time

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def bs_call(S, K, T, sigma):
    d1 = (np.log(S / K) + (0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    call_price = S * norm.cdf(d1) - K * norm.cdf(d2)
    return call_price

def bs_put(S, K, T, sigma):
    d1 = (np.log(S / K) + (0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    put_price = K * norm.cdf(-d2) - S * norm.cdf(-d1)
    return put_price

def implied_volatility_call(S, K, T, C):
    def f(sigma):
        return bs_call(S, K, T, sigma) - C
    return brentq(f, 0.00001, 10000.0)

def implied_volatility_put(S, K, T, P):
    def f(sigma):
        return bs_put(S, K, T, sigma) - P
    return brentq(f, 0.00001, 10000.0)

def redis_dates():
    return redis_client.get("option_contracts_active").decode("utf-8")

def redis_prices():
    contracts = redis_contracts()

    answer = {}
    simple_list = []
    try:
        for el in contracts:
            contracts_exp_list = contracts[el]
            for item in contracts_exp_list:
                simple_list.append(item)
                values = redis_client.get(item).decode("utf-8")
                dvalues = json.loads(values)
                itm_list=item.split("-")
                
                #print("the values", dvalues, type(dvalues))
                #print("bids", dvalues["bids"])
                #print("asks", dvalues["asks"])
                if len(dvalues["bids"])>0:
                    dvalues["bids"]=[dvalues["bids"][0]]
                if len(dvalues["asks"])>0:
                    dvalues["asks"]=[dvalues["asks"][0]]
                #print("="*20)

                dvalues["edate"]=itm_list[1]
                dvalues["strk"]=itm_list[2]
                #print(dvalues, el, "\n")
                
                answer[item]=dvalues
                
                
    except Exception as e:
        print(e)
    return answer, simple_list

def redis_contracts():
    answer = {}
    dates = redis_dates()
    for el in json.loads(dates):
        contracts = redis_client.get(f"worker:{el}").decode("utf-8")

        contracts_dict = json.loads(contracts)
        #print("supposedly dict:", type(contracts_dict), contracts_dict)
        answer[el] = contracts_dict
    return answer

def calculate_iv(contracts):
    spot = json.loads(redis_client.get("ethusdt").decode('utf-8'))
    sbid = float(spot['bids'][0][0])
    sask = float(spot['asks'][0][0])

    contract_with_prices = {}
    for symbol in contracts:
        #print(symbol)        
        price_data = redis_client.get(symbol) #Get data as a string
        #print("# the price data:", price_data)
        if price_data:
            print(symbol)

            price_data_js = json.loads(price_data.decode('utf-8')) #Convert to dictionary
            itm_list = symbol.split("-")
            price_data_js["edate"]=itm_list[1]
            price_data_js["strk"]=itm_list[2]
            
            print("!!!", price_data_js)
            if len(price_data_js["asks"])>0:
                price_data_js["asks"]=[price_data_js["asks"][0]]
            if len(price_data_js["bids"])>0:
                price_data_js["bids"]=[price_data_js["bids"][0]]

            #contract_with_prices[symbol]=price_data.decode('utf-8')
            
            #print(price_data.decode('utf-8'), type( json.loads(contract_with_prices[symbol] )))

            bids = price_data_js['bids']
            asks = price_data_js['asks']
            if bids!=[] and asks!=[] and symbol != "ethusdt":
                #print("calculating IV main part")
                bid = bids[0][0]
                ask = asks[0][0]
                mid_stock_price = (sask+sbid)/2

                #print(symbol.split("-"))
                K=float(symbol.split("-")[2]) #Strike price

                contract_date = symbol.split("-")[1]
                contract_date_obj = datetime.strptime(contract_date, "%y%m%d")
                current_date = datetime.now()
                difference = contract_date_obj-current_date
                days_difference=difference.days
                #print(days_difference)
                if days_difference<=0: days_difference=1

                T=days_difference/365
                
                iv_bid=0
                iv_ask=0
                if symbol.split("-")[3]=="P":
                    try:
                        iv_bid = implied_volatility_put(mid_stock_price, K, T, float(bid))
                    except Exception as e:
                        print("e1", e)
                    try:
                        iv_ask = implied_volatility_put(mid_stock_price, K, T, float(ask))
                    except Exception as e:
                        print("e2", e)

                if symbol.split("-")[3]=="C":
                    try:
                        iv_bid = implied_volatility_call(mid_stock_price, K, T, float(bid))
                    except Exception as e:
                        print("e3", e)

                    try:
                        iv_ask = implied_volatility_call(mid_stock_price, K, T, float(ask))
                    except Exception as e:
                        print("e4", e)

                price_data_js['iv']={'iv_bid':iv_bid,'iv_ask':iv_ask }
                #print('#!!! price data:', price_data_js)
                contract_with_prices[symbol]=price_data_js
    return contract_with_prices
 
def calc_all():
    st=datetime.now()
    prices, contracts = redis_prices()
    elspd = datetime.now()-st
    
    st2=datetime.now()
    iv = calculate_iv(contracts)
    elspd2 = datetime.now()-st2

    response={
            'delay': elspd.total_seconds(),
            'delay_iv': elspd2.total_seconds(),
            'command': 'get_all_prices',
            #'prices': prices,
            'iv': iv,
            'generated': str(datetime.now())
        }

    values = json.dumps(response)
    redis_client.set("calculated_data", values)

while True:
    calc_all()
    print(datetime.now(), "calculated")
    
    time.sleep(5)
    