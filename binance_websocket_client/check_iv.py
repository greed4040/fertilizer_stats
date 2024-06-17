import redis
import json
from datetime import datetime

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq

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
    return brentq(f, 0.0001, 1000.0)

def implied_volatility_put(S, K, T, P):
    def f(sigma):
        return bs_put(S, K, T, sigma) - P
    return brentq(f, 0.0001, 1000.0)

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
contracts={}

# Read contracts from file
def read_contracts(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    # Извлекаем контракты из ключа 'mindate_contracts'
    contracts = data.get('mindate_contracts', [])
    return contracts

def run(contracts):
    spot = json.loads(redis_client.get("ethusdt").decode('utf-8'))
    sbid = float(spot['bids'][0][0])
    sask = float(spot['asks'][0][0])
    
    print(contracts, type(contracts), sbid, sask)
    contract_with_prices={}
    for symbol in contracts:
        print(symbol)        
        price_data = redis_client.get(symbol) #Get data as a string
        
        price_data_string = price_data.decode('utf-8') #Convert to utf-8 string
        price_data = json.loads(price_data_string) #Convert to dictionary
        
        #contract_with_prices[symbol]=price_data.decode('utf-8')
        
        #print(price_data.decode('utf-8'), type( json.loads(contract_with_prices[symbol] )))
        bids = price_data['bids']
        asks = price_data['asks']  
        if bids!=[] and asks!=[]:
            print("-"*30)
            print("main part")

            bid = bids[0][0]
            ask = asks[0][0]
            mid_stock_price = (sask + sbid)/2

            print(symbol.split("-"))
            K=float(symbol.split("-")[2]) #Strike price
            print(f"K strike price: {K}")

            contract_date = symbol.split("-")[1]
            contract_date_obj = datetime.strptime(contract_date, "%y%m%d")
            current_date = datetime.now()
            difference = contract_date_obj- current_date
            days_difference=difference.days
            if days_difference==0: days_difference=1
            T=days_difference/365
            print(f"T days_difference: {T}")
            
            iv_bid=0
            iv_ask=0
            if symbol.split("-")[3]=="P":
                iv_bid = implied_volatility_put(mid_stock_price, K, T, float(bid))
                iv_ask = implied_volatility_put(mid_stock_price, K, T, float(ask))
            if symbol.split("-")[3]=="C":
                iv_bid = implied_volatility_call(mid_stock_price, K, T, float(bid))
                iv_ask = implied_volatility_call(mid_stock_price, K, T, float(ask))
            price_data['iv']={'iv_bid':iv_bid,'iv_ask':iv_ask }
            print(price_data)
            contract_with_prices[symbol]=json.dumps(price_data)

    contracts.append("ethusdt")
    price_data = redis_client.get(symbol)
    contract_with_prices[symbol]=price_data.decode('utf-8')

contracts = read_contracts('current_option_min_date_contracts_eth.json')
run(contracts)