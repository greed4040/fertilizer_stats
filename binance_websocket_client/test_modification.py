import utils_helper as uh

import redis

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
current_contracts_date = int(redis_client.get("option_contracts_date").decode('utf-8'))
spot_prefix = "ETH"
spot_contract = "ETHUSDT"
contracts_file = "contracts.json"
limit = 5

#d = "1716710400000"
#print(new_contracts_date)
#r = uh.extract_filtered_contracts_by_expdate_into_file(new_contracts_date, "ETHUSDT", "ETH", 5)
#print(r)
#r = uh.get_contracts_by_expiry_prefix_and_date(new_contracts_date, spot_prefix, "contracts.json")
#print(r)

#print(uh.get_contracts_only())



contracts = uh.get_contracts_by_expiry_prefix_and_date(current_contracts_date, spot_prefix, contracts_file)
avg_prc = uh.get_avg_price(spot_contract)
returned_data = uh.get_outOfTheMoney_strikes_puts_calls_list_V2(contracts, avg_prc, limit)
print(returned_data)


uh.get_contracts_only()
