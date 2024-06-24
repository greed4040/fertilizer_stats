from flask import Flask
from flask import json
from flask import request
from flask import jsonify
from flask_cors import CORS
import time
from datetime import datetime
#import mysql.connector
import string
import random
import hashlib

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from scipy.optimize import newton

import redis

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Read contracts from file
#the_date = redis_client.get("option_contracts_date").decode('utf-8')


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

@app.route('/get_selected_date', methods=['GET'])
def get_selected_date():
    print("#get_selected_date request", request, datetime.now())
    option_contracts_date = uh2.get_current_expiry_date_from_redis()
    dt_strirng = datetime.fromtimestamp(int(option_contracts_date)/1000).strftime("%Y %m %d")
    response = {
        "status": "ok",
        "option_contracts_date": option_contracts_date,
        "option_contracts_dt":dt_strirng,
        "dt":datetime.now(),
        "command":'get_selected_date'
    }
    return jsonify(response)

@app.route('/get_all_filtered_read_file_only_symbols_only', methods=['GET'])
def get_all_filtered_read_file_only_symbols_only():
    print("#get_all_filtered_read_file_only_symbols_only request", request, datetime.now())
    
    resp = uh2.get_all_filtered_read_file_only_symbols_only()
    print(resp)
    #eth_keys=eth_keys.sort()
    response = {
        "status": "ok",
        "eth_keys": resp,
        "dt": datetime.now(),
        "command": 'get_all_filtered_read_file_only_symbols_only'
    }

    #print(response)
    return jsonify(response)

@app.route('/get_all_redis_date_contracts_from_file', methods=['GET'])
def get_all_redis_date_contracts_from_file():
    print("#get_all_redis_date_contracts_from_file request", request, datetime.now())
    
    redis_date = uh2.get_current_expiry_date_from_redis()

    resp = uh2.get_contracts_by_expiry_date_and_prefix_name_only(redis_date, "ETH")

    print(resp)
    #eth_keys=eth_keys.sort()
    response = {
        "status": "ok",
        "eth_keys": resp,
        "dt":datetime.now(),
        "command":'get_all_filtered_read_file_only_symbols_only'
    }

    #print(response)
    return jsonify(response)

@app.route('/get_all_expiry_dates', methods=['GET'])
def get_all_expiry_dates():
    print("#get_all_dates request", request, datetime.now())
    #date = redis_client.get("option_contracts_date").decode('utf-8')
    expiry_dates = uh2.get_all_expiry_dates()
    #print("#", expiry_dates)
    expiry_dates.sort()
    #print("##", expiry_dates)
    response={
            'data':expiry_dates,
            'dt':datetime.now(),
            'command':'get_all_expiry_dates'
        }
    return jsonify(response)

###
###
###

@app.route('/get_symbol_data', methods=['POST'])
def get_symbol_data():
    print("get_symbol_data")
    contracts = request.json.get('data')
    print("#contracts: ", contracts)
    
    spot = json.loads(redis_client.get("ethusdt").decode('utf-8'))
    sbid = float(spot['bids'][0][0])
    sask = float(spot['asks'][0][0])
    
    print("#!!", contracts, type(contracts), sbid, sask)
    contract_with_prices={}
    for symbol in contracts:
        print(symbol)        
        price_data = redis_client.get(symbol) #Get data as a string
        print("# the price data:", price_data)
        if price_data:
            price_data_js = json.loads(price_data.decode('utf-8')) #Convert to dictionary
        
            #contract_with_prices[symbol]=price_data.decode('utf-8')
            
            #print(price_data.decode('utf-8'), type( json.loads(contract_with_prices[symbol] )))
            bids = price_data_js['bids']
            asks = price_data_js['asks']  
            if bids!=[] and asks!=[] and symbol != "ethusdt":
                print("calculating main part")
                bid = bids[0][0]
                ask = asks[0][0]
                mid_stock_price = (sask+sbid)/2

                print(symbol.split("-"))
                K=float(symbol.split("-")[2]) #Strike price

                contract_date = symbol.split("-")[1]
                contract_date_obj = datetime.strptime(contract_date, "%y%m%d")
                current_date = datetime.now()
                difference = contract_date_obj-current_date
                days_difference=difference.days
                print(days_difference)
                if days_difference<=0: days_difference=1

                T=days_difference/365
                
                iv_bid=0
                iv_ask=0
                if symbol.split("-")[3]=="P":
                    try:
                        iv_bid = implied_volatility_put(mid_stock_price, K, T, float(bid))
                    except Exception as e:
                        print(e)
                    try:
                        iv_ask = implied_volatility_put(mid_stock_price, K, T, float(ask))
                    except Exception as e:
                        print(e)

                if symbol.split("-")[3]=="C":
                    try:
                        iv_bid = implied_volatility_call(mid_stock_price, K, T, float(bid))
                    except Exception as e:
                        print(e)

                    try:
                        iv_ask = implied_volatility_call(mid_stock_price, K, T, float(ask))
                    except Exception as e:
                        print(e)

                price_data_js['iv']={'iv_bid':iv_bid,'iv_ask':iv_ask }
                #print('#!!! price data:', price_data_js)
                contract_with_prices[symbol]=json.dumps(price_data_js)
            
    contracts.append("ethusdt")
    #print("the contracts:", contracts)
    spot_price_data = redis_client.get("ethusdt")
    contract_with_prices[symbol]=spot_price_data.decode('utf-8')
    #print("contracts with prices:", contract_with_prices)
    return jsonify({"command":"get_symbol_data", "status": "ok", "data": contract_with_prices, "dt": datetime.now()})

@app.route('/submit_date', methods=['POST'])
def submit_date():
    print("submit_date")
    answer = "started"

    recieved_data = request.json.get('data')
    print(f"recieved_data: {recieved_data}")
    
    current_date = redis_client.get("option_contracts_date").decode('utf-8')
    print(current_date)
    old_contracts_date = int()
    redis_client.set("option_contracts_date", recieved_data)
    new_contracts_date = int(redis_client.get("option_contracts_date").decode('utf-8'))
    
    spot_prefix = "ETH"
    spot_contract = "ETHUSDT"

    res = uh2.get_all_filtered_redis_date_contracts_from_file(depth=2)
    print(res)

    answer={
            "status": res, 
            "command":"submit_date", 
            "dt": datetime.now(),
            "old_date": old_contracts_date,
            "new_date": new_contracts_date,
            "is_updated": old_contracts_date != new_contracts_date
        }
    
    print(answer)
    return jsonify(answer)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9019, debug=True, ssl_context=('../../../certs/fullchain.pem', '../../../certs/privkey.pem'))
