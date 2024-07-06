import mysql.connector

# Set up MySQL connection
db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'  # Specify the database here
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Execute the SHOW DATABASES command
cursor.execute("select * from eth_prices where id = (select max(id) from eth_prices);")

# Fetch and print all databases
ans = cursor.fetchall()
for el in ans:
    print(el)

# Close the connection
cursor.close()
conn.close()


