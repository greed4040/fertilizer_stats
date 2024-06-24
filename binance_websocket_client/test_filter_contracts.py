import utils_helper2 as uh2
import redis

#uh2.set_min_expiry_date_to_redis()

#aaa = uh2.get_all_redis_date_contracts_from_file("ETH")
#print(aaa)



#cdate = uh2.get_current_expiry_date_from_redis()
#print("#cdate", cdate, type(cdate))
all_dates = uh2.get_all_expiry_dates()
print("#all dates", all_dates, type(all_dates[0]))
#print(cdate, "\n", all_dates, "\n")
#print("#is redis date in all dates:", int(cdate) in all_dates)

resp = uh2.read_date_from_redis_and_generate_filtered_list_around_current_price(all_dates[0], "ETH", 2)
for r in resp:
    print(r, len(resp[r]))
    if len(resp[r])>0:
        lst = resp[r]
        for el in lst:
            print(el["strikePrice"], el["side"])
            print(el,"\n\n")
print("="*20,"\n\n")

#print(uh2.get_all_filtered_read_file_only_symbols_only())

all_dates = uh2.get_all_expiry_dates()
#current = uh2.get_current_expiry_date_from_redis()

new_date = all_dates[0]

#print(new_date, current)

redis_client = redis.Redis(host='localhost', port=6379, db=0)
redis_client.set("option_contracts_date", new_date)
aaa = uh2.read_date_from_redis_generate_filtered_list_around_current_price(new_date)
print(aaa)
