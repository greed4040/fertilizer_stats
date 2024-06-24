import redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

worker_keys = redis_client.keys("worker*")

for el in worker_keys:
    dec_el = el.decode('utf-8')
    print(dec_el)
    redis_client.delete(dec_el)
