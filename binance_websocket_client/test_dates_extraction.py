import utils_helper2 as uh2
import datetime

r = uh2.get_expiry_dates("contracts.json")
for d in r:
    dt_object = datetime.datetime.fromtimestamp(d/1000)
    # Print the datetime object in a human-readable format
    # You can also format the datetime object according to your needs
    print(d, dt_object.strftime("%Y-%m-%d %H:%M:%S"))  # Year-Month-Day Hour:Minute:Second

print(uh2.get_all_filtered_read_file_only())