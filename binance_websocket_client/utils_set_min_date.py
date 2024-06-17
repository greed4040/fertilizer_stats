import utils_helper2 as uh2

print(uh2.get_all_expiry_dates())
print(uh2.get_min_expiry_date())
print("it is now:", uh2.get_current_expiry_date_from_redis())
uh2.set_min_expiry_date_to_redis()
print("changed to:", uh2.get_current_expiry_date_from_redis())
