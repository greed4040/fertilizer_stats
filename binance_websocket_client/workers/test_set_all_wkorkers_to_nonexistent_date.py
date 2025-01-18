import redis
import json

# Set up Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# The date to set for the workers
date_to_set = 1722067200000

# The worker keys to update
worker_keys = [f"record_id_{i}" for i in range(4)]

def set_worker_dates(worker_keys, date):
    for worker_key in worker_keys:
        # Set the worker date
        redis_client.set(worker_key, date)
        print(f"Set {worker_key} to date {date}")

# Run the function to set the dates
set_worker_dates(worker_keys, date_to_set)
