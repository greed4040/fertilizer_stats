import requests
import mysql.connector
import pandas as pd
import time
from datetime import datetime, timedelta

db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'
}

symbol = 'ETHUSDT'
interval = '1h'
start_date = '2023-06-01'

db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'
}

symbol = 'ETHUSDT'
interval = '1h'
start_date = '2023-06-01'

def get_binance_ohlc_data(symbol, interval='1h', start_time=None, end_time=None, limit=1000):
    url = f"https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'limit': limit
    }
    if start_time:
        params['startTime'] = int(start_time.timestamp() * 1000)
    if end_time:
        params['endTime'] = int(end_time.timestamp() * 1000)

    response = requests.get(url, params=params)
    data = response.json()
    
    if response.status_code != 200:
        print(f"Error fetching data: {data}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame в случае ошибки
    
    # Убедимся, что данные имеют правильный формат и количество столбцов
    if len(data) == 0 or len(data[0]) != 12:
        print(f"Unexpected data format: {data}")
        return pd.DataFrame()
    
    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume', 
        'close_time', 'quote_asset_volume', 'number_of_trades', 
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    
    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']]
    
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(float)
    
    return df

def save_to_mysql(df, symbol, connection):
    cursor = connection.cursor()
    for _, row in df.iterrows():
        print(f"Inserting row: {row.to_dict()}")
        cursor.execute("""
            INSERT INTO ohlc_data (symbol, open_time, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            open = VALUES(open), high = VALUES(high), low = VALUES(low), close = VALUES(close), volume = VALUES(volume)
        """, (symbol, int(row['open_time']), row['open'], row['high'], row['low'], row['close'], row['volume']))
    connection.commit()

def get_last_stored_time(symbol, connection):
    cursor = connection.cursor()
    cursor.execute(f"SELECT MAX(open_time) FROM ohlc_data WHERE symbol = '{symbol}'")
    result = cursor.fetchone()
    if result[0]:
        return datetime.fromtimestamp(result[0] / 1000)
    else:
        return datetime.strptime(start_date, '%Y-%m-%d')

def main():
    connection = mysql.connector.connect(**db_config)
    last_stored_time = get_last_stored_time(symbol, connection)
    current_time = datetime.now()

    while last_stored_time < current_time:
        end_time = min(last_stored_time + timedelta(days=30), current_time)
        df = get_binance_ohlc_data(symbol, interval, start_time=last_stored_time, end_time=end_time)
        if not df.empty:
            save_to_mysql(df, symbol, connection)
            last_stored_time = datetime.fromtimestamp(df['open_time'].iloc[-1] / 1000)
        time.sleep(1)  # Делей для предотвращения блокировки API Binance
    
    connection.close()

if __name__ == "__main__":
    main()
