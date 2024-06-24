import utils_workers_helper as uwh
import json
import datetime
import os
#storage = "option_contract_dates" 
temp_storage = "test_dates_storage"

def manage_lock_and_update_redis():
    lock_acquired = uh2.acquire_lock_with_timeout(storage)
    if not lock_acquired:
        print("Could not acquire lock, exiting...")
        return

    try:
        # Get all expiry dates
        all_dates = uh2.get_all_expiry_dates()
        print("# all dates from file:", all_dates)

        all_stored_contracts = uh2.hkeys_from_redis(storage)
        #print(all_stored_contracts)
        all_contracts = [int(el.decode('utf-8')) for el in all_stored_contracts]
        print("# all contracts in db:", all_contracts)
        for el in all_contracts:
            #print(el in all_contracts)
            if el not in all_dates:
                uh2.hdel_field_from_key_in_redis(storage, el)
                print(f"# removing field {el}")
        
        # Iterate through all_dates and set each one into Redis
        for id, el in enumerate(all_dates):
            if el not in all_contracts:
                dt=datetime.datetime.fromtimestamp(int(el)/1000).strftime("%Y-%m-%d %H:%M:%S")
                fields_values = json.dumps({ "id": f"id_{id}", "busy": "no", "dt": dt })
                uh2.hset_value_to_field_by_key_in_redis(storage, el, fields_values)
                print(storage, el, fields_values)
    finally:
        # Ensure the lock is released
        uh2.release_lock("option_contract_dates")

#manage_lock_and_update_redis()

print(f"my pid:{os.getpid()}")
def reserve():
    selected_date = uwh.select_date(1, temp_storage)
    print(f"#did we get a date? {selected_date}")

