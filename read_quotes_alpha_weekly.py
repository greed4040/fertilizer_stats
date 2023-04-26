import requests
import configparser as c
import json
import time
cfg = c.ConfigParser()
cfg.read('config.ini')
alpha_key=cfg['api']['alphavantage']

symbol="ETH"
market="USD"
# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
#url = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol={}&market={}&apikey={}'.format(symbol, market, alpha_key)
#url = 'https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={}&interval=5min&apikey={}'.format(symbol, alpha_key)


symbols=["VALE","NTR","MOS","ICL","IPI","CMP","BHP","SPY","SOIL","MOO"]

for id in range(0,len(symbols)):
    symbol=symbols[id]
    url='https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={}&apikey={}'.format(symbol, alpha_key)
    print(url)
    r = requests.get(url)
    data = r.json()


    filename="quotes_weekly_{}.txt".format(symbol)
    with open(filename, 'w') as convert_file:
        convert_file.write(json.dumps(data))
    for i in range(20):
        print(i)
        time.sleep(1)
print(data)
