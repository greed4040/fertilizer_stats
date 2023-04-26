import json
symbols=["SPY","VALE","NTR","MOS","ICL","IPI","CMP","BHP","SOIL","MOO"]

suffix="quotes_weekly_"

dic={}
for sym in symbols:
    filename = suffix+sym+".txt"
    result=""
    with open(filename, 'r') as file_data:
        result=file_data.read()
    #print(result)
    #break
    result = result.replace("1. ","")
    result = result.replace("2. ","")
    result = result.replace("3. ","")
    result = result.replace("4. ","")
    result = result.replace("5. ","")
    rslt=json.loads(result)['Weekly Time Series']
    dic[sym]=rslt
    print("#"*20, sym)
    #print(rslt)


merged_dict={}
for date in dic[symbols[0]].keys():
    price={}
    for sym in symbols:
        if date in dic[sym]:
            price[sym]=dic[sym][date]['close']
            print(date, len(price))
        else:
            price[sym]=None
            #print(date)
    '''
    price0=dic[symbols[0]][date]['close']
    if date in dic[symbols[1]]:
        price1 = dic[symbols[1]][date]['close']
    else:
        price1 = None

    if date in dic[symbols[2]]:
        price2 = dic[symbols[2]][date]['close']
    else:
        price2 = None
    '''
    

    merged_dict[date]=price        


import pandas as pd

def dict_to_css(d, filename):
    # Convert dictionary to DataFrame
    df = pd.DataFrame.from_dict(d, orient='index')

    # Save DataFrame to CSS file
    df.to_csv(filename, sep=',', header=True, index=True)

dict_to_css(merged_dict, 'table_output.css')



