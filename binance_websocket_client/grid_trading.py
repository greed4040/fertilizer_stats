import mysql.connector
from decimal import Decimal
import pandas as pd

# Database configuration
db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'
}

# Trading configuration
trading_config = {
    "initial_price": 3400,
    "offset": 10,
    "levels_count": 10,
    "volume": 10,
    "precision": 4,
    "side": "Buy",
    "pair": "USDT_ETH",
    "take_profit": 10,
    "commission": Decimal(0.001)  # Commission rate, e.g., 0.1%
}

def generate_grid(config):
    grid = []
    for i in range(config['levels_count']):
        price = config['initial_price'] + (i - config['levels_count']//2) * config['offset']
        grid.append(round(Decimal(price), config['precision']))
    return sorted(grid)

def execute_buy(price, volume, commission):
    cost = price * volume
    cost_with_commission = cost * (1 + commission)
    return price, volume, cost_with_commission

def execute_sell(buy_price, sell_price, volume, commission):
    revenue = sell_price * volume
    revenue_with_commission = revenue * (1 - commission)
    profit = revenue_with_commission - buy_price * volume
    return profit

def calculate_unrealized_pl(positions, current_bid):
    return sum((current_bid - buy_price) * volume for buy_price, volume, _ in positions)

def run_grid_trading():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Generate the grid
    grid = generate_grid(trading_config)
    print("Grid levels:")
    for level in grid:
        print(f"Level: {level}")

    # Confirm to proceed
    confirm = input("Do you want to start the simulation? (yes/no): ")
    if confirm.lower() != 'yes':
        print("Simulation aborted.")
        cursor.close()
        conn.close()
        return

    positions = []
    realized_profit = Decimal('0')
    unrealized_profit = Decimal('0')
    closed_trades_count = 0  # Counter for closed trades

    # Get all records
    cursor.execute("SELECT * FROM eth_prices ORDER BY id")
    
    for record in cursor:
        id, timestamp, bid, bid_size, ask, ask_size = record
        if id%1000==0:
            print(f"\nProcessing record ID: {id}, Time: {timestamp}")
            print(f"Current price - Bid: {bid}, Ask: {ask}")

        # Check for buy opportunities
        for level in grid:
            if ask <= level and not any(pos[0] == level for pos in positions):
                volume = trading_config['volume'] / level
                buy_position = execute_buy(level, volume, trading_config['commission'])
                positions.append(buy_position)
                #print(f"Bought at grid level {level}. Volume: {volume}, Cost with Commission: {buy_position[2]}")

        # Check for sell opportunities
        positions_to_remove = []
        for i, (buy_price, volume, cost_with_commission) in enumerate(positions):
            sell_price = buy_price + trading_config['take_profit']
            if bid >= sell_price:
                profit = execute_sell(buy_price, sell_price, volume, trading_config['commission'])
                realized_profit += profit
                closed_trades_count += 1  # Increment closed trades counter
                #print(f"Sold at {sell_price}. Bought at {buy_price}. Realized Profit: {profit}")
                positions_to_remove.append(i)

        # Remove sold positions
        for i in sorted(positions_to_remove, reverse=True):
            positions.pop(i)

        # Calculate unrealized P/L
        unrealized_profit = calculate_unrealized_pl(positions, bid)

        # Print open positions as a table
        positions_df = pd.DataFrame(positions, columns=['Buy Price', 'Volume', 'Cost with Commission'])
        
        if id%1000==0:
            print("\nCurrent open positions:")
            print(positions_df)

            print(f"Cumulative Realized P/L: {realized_profit}")
            print(f"Current Unrealized P/L: {unrealized_profit}")
            print(f"Total P/L (Realized + Unrealized): {realized_profit + unrealized_profit}")

    print("\nFinal Results:")
    print(f"Total Realized Profit: {realized_profit}")
    print(f"Final Unrealized Profit: {unrealized_profit}")
    print(f"Total P/L (Realized + Unrealized): {realized_profit + unrealized_profit}")
    print(f"Remaining open positions: {len(positions)}")
    print(f"Total closed trades: {closed_trades_count}")  # Print total closed trades

    cursor.close()
    conn.close()

# Run the grid trading simulation
run_grid_trading()
