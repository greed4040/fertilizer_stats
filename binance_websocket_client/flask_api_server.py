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

from decimal import Decimal
import redis
import mysql.connector

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

def redis_prices():
    contracts = redis_contracts()

    answer = {}
    simple_list = []
    for el in contracts:
        contracts_exp_list = contracts[el]
        for item in contracts_exp_list:
            simple_list.append(item)
            values = redis_client.get(item).decode("utf-8")
            #print(values)
            answer[item]=json.loads(values)
    return answer, simple_list

def calculate_iv(contracts):
    spot = json.loads(redis_client.get("ethusdt").decode('utf-8'))
    sbid = float(spot['bids'][0][0])
    sask = float(spot['asks'][0][0])

    contract_with_prices = {}
    for symbol in contracts:
        print(symbol)        
        price_data = redis_client.get(symbol) #Get data as a string
        #print("# the price data:", price_data)
        if price_data:
            price_data_js = json.loads(price_data.decode('utf-8')) #Convert to dictionary
        
            #contract_with_prices[symbol]=price_data.decode('utf-8')
            
            #print(price_data.decode('utf-8'), type( json.loads(contract_with_prices[symbol] )))
            bids = price_data_js['bids']
            asks = price_data_js['asks']  
            if bids!=[] and asks!=[] and symbol != "ethusdt":
                #print("calculating main part")
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
                contract_with_prices[symbol]=price_data_js
    return contract_with_prices
       
@app.route('/get_all_contracts', methods=['GET'])
def get_all_contracts():
    print("="*30)
    print("#get_all_contracts request", request, datetime.now())
    
    answer = redis_contracts()

    response = {
        "status": "ok",
        "symbols": answer,
        "dt":datetime.now(),
        "command":'get_all_contracts'
    }

    print("\n", response)
    return jsonify(response)

@app.route('/get_all_expiry_dates', methods=['GET'])
def get_all_expiry_dates():
    print("#get_all_dates request", request, datetime.now())
    #date = redis_client.get("option_contracts_date").decode('utf-8')
    expiry_dates_str = redis_dates()
    expiry_dates = json.loads(expiry_dates_str)
    #print("#", expiry_dates)
    expiry_dates.sort()
    #print("##", expiry_dates)
    response={
            'data':expiry_dates,
            'dt':datetime.now(),
            'command':'get_all_expiry_dates'
        }
    return jsonify(response)

@app.route('/get_all_prices', methods=['GET'])
def get_all_prices():
    print("#get_all_prices request", request, datetime.now())

    data_str = redis_client.get("calculated_data").decode("utf-8")
    data_dict = json.loads(data_str)
    print(data_dict)
    response={
        'data':data_dict,
        'dt':datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        'command':'get_all_prices'
    }
    return jsonify(response)

@app.route('/get_spot', methods=['GET'])
def get_spot():
    print("#get_spot_request", request, datetime.now())

    spot_price = redis_client.get("eth_spot_price").decode("utf-8")
    response = json.loads(spot_price)
    response["dt"] = datetime.now()

    response={
        'data':spot_price,
        'dt':datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        'command':'get_spot'
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


##
##
##
##

db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'
}

def get_data_from_db(contract):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    
    query = f"""
        SELECT datetime, bid, bid_size, ask, ask_size 
        FROM {contract[:3]}_prices 
        WHERE bid > 2000 AND ask > 2000 AND MOD(id, 100) = 0 
        ORDER BY id DESC
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return rows

def convert_decimal_to_float(data):
    for row in data:
        for key in row:
            if isinstance(row[key], Decimal):
                row[key] = float(row[key])
    return data

@app.route('/get_data/<contract>', methods=['GET'])
def get_data(contract):
    data = get_data_from_db(contract)
    data = convert_decimal_to_float(data)
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9019, debug=True, ssl_context=('../../../certs/fullchain.pem', '../../../certs/privkey.pem'))
