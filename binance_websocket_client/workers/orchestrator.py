import redis
import json
import time
import datetime

# Set up Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Constants
OPTION_CONTRACTS_ACTIVE = "option_contracts_active"
WORKER_KEY_PREFIX = "record_id_"

def unix_to_formatted_date(unix_seconds):
    # Convert Unix timestamp to a datetime object
    dt = datetime.datetime.fromtimestamp(unix_seconds)
    # Format the datetime object to "yy-mm-dd"
    formatted_date = dt.strftime("%Y-%m-%d")
    return formatted_date

def get_active_contract_dates():
    """Fetch the list of active contract dates from Redis."""
    active_dates = redis_client.get(OPTION_CONTRACTS_ACTIVE).decode('utf-8')
    return json.loads(active_dates)

def get_assigned_dates():
    """Fetch the currently assigned dates for all workers."""
    assigned_dates = {}
    for key in redis_client.scan_iter(f"{WORKER_KEY_PREFIX}*"):
        #print("key", key)
        key_decoded = key.decode("utf-8")
        #print("key_decoded", key_decoded)
        date = json.loads(redis_client.get(key).decode("utf-8"))
        #print("date", date, type(date))
        assigned_dates[key_decoded] = date

    #print("!!!###", assigned_dates)
    return {key: assigned_dates[key] for key in sorted(assigned_dates)}

def assign_dates_to_workers_suggested(workers):
    """Assign dates to workers based on active contract dates."""
    active_dates = get_active_contract_dates()
    assigned_dates = get_assigned_dates()

    # Create a mapping of active dates to workers
    date_worker_map = {date: worker for worker, date in assigned_dates.items()}

    for worker in workers:
        if worker in assigned_dates:
            # Check if the assigned date is still active
            assigned_date = assigned_dates[worker]
            if assigned_date not in active_dates:
                # Reassign the worker to a new date
                available_dates = [date for date in active_dates if date not in date_worker_map]
                if available_dates:
                    new_date = available_dates[0]
                    date_worker_map[new_date] = worker
                    redis_client.set(f"{WORKER_KEY_PREFIX}{worker}", json.dumps({"d": new_date}))
                    print(f"Reassigned worker {worker} to date {new_date}")
                else:
                    print(f"No available dates to reassign worker {worker}")
        else:
            # Assign the worker to an available date
            available_dates = [date for date in active_dates if date not in date_worker_map]
            if available_dates:
                new_date = available_dates[0]
                date_worker_map[new_date] = worker
                redis_client.set(f"{WORKER_KEY_PREFIX}{worker}", json.dumps({"d": new_date}))
                print(f"Assigned worker {worker} to date {new_date}")
            else:
                print(f"No available dates to assign to new worker {worker}")

def assign_dates_to_workers(workers):
    dt_active = get_active_contract_dates()
    dt_active_human = [unix_to_formatted_date(int(el)/1000) for el in dt_active]
    print("# active dates human:", dt_active_human)

    for i in range(len(dt_active)):
        print("# active dates:", dt_active[i], unix_to_formatted_date(dt_active[i]/1000))
    

    dt_assigned = get_assigned_dates()
    #print("###", dt_assigned)
    for el in dt_assigned:
        #print(el)
        #print(dt_assigned[el])
        print(f"# assigned: {el} {dt_assigned[el]} {unix_to_formatted_date(dt_assigned[el]/1000)} {'' if dt_assigned[el] in dt_active else '* <-- the worker is set to inactive'}")
    
    dates_assigned = [dt_assigned[dt] for dt in dt_assigned]
    inactive_dates = [dt for dt in dt_active if dt not in dates_assigned]
    
    print("--- main execution ---")
    print(f"# These are inactive datees: {inactive_dates}")
    for wk in dt_assigned:
        if dt_assigned[wk] in dt_active:
            print(f"# lp # we're all set: {wk} {dt_assigned[wk]}")
        else:
            print("---")
            print(f"# lp # {wk}")
            print(f"# lp # looking for empty date: worker is set to {dt_assigned[wk]} which is {unix_to_formatted_date(dt_assigned[wk]/1000)}")
            print(f"# lp # It's not active. The list is: {dt_active}")
            print(f"# lp # which is {dt_active_human}")
            if len(inactive_dates)>0:
                first_inactive = inactive_dates[0]
                print(f"# lp # condition # got inactive dates list: {inactive_dates}")
                print(f"# lp # first inactive date is {inactive_dates[0]} which is {unix_to_formatted_date(first_inactive/1000)}")
                print(f"# lp # condition # assigning {wk} to date {inactive_dates[0]} which is {unix_to_formatted_date(first_inactive/1000)}")
                
                redis_client.set(wk, first_inactive)
                break

def orchestrator():
    """Main orchestrator function to assign dates to workers."""
    while True:
        # List of workers to manage (for example purposes, assume a fixed list)
        workers = ["worker0", "worker1", "worker2", "worker3"]

        assign_dates_to_workers(workers)

        break
        # Sleep for a while before the next check
        #time.sleep(60)

if __name__ == "__main__":
    orchestrator()

