import redis
import sys
import json

def main():
    # Check if an index argument is provided
    if len(sys.argv) != 3:
        print("Usage: python dates_switch.py <worker_id> <index>")
        sys.exit(1)

    try:
        worker_id = int(sys.argv[1])
        index = int(sys.argv[2])
    except ValueError:
        print("Index must be an integer.")
        sys.exit(1)

    # Set up Redis connection
    redis_client = redis.Redis(host='localhost', port=6379, db=0)

    # Read the list from Redis
    list_key = "option_contracts_active"
    try:
        dates_list_binary = redis_client.get(list_key)
    except Exception as e:
        print(f"Failed to read the list from Redis: {e}")
        sys.exit(1)

    print(dates_list_binary, type(dates_list_binary))
    dates_str = dates_list_binary.decode("utf-8")
    print(dates_str, type(dates_str))
    dates_list = json.loads(dates_str)
    print(dates_list, type(dates_list))
    

    # Check if the provided index is valid
    if index < 0 or index >= len(dates_list):
        print(f"Index out of range. Valid range is 0 to {len(dates_list) - 1}.")
        sys.exit(1)

    # Extract the date from the list using the provided index
    selected_date = dates_list[index]

    # Write the selected date to `record_id_1` in Redis
    record_key = f"record_id_{worker_id}"
    try:
        redis_client.set(record_key, selected_date)
    except Exception as e:
        print(f"Failed to write the date to Redis: {e}")
        sys.exit(1)

    print(f"Successfully wrote date '{selected_date}' to '{record_key}'.")

if __name__ == "__main__":
    main()

