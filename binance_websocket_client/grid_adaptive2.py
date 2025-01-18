import mysql.connector
from decimal import Decimal
import pandas as pd
import numpy as np

# Database configuration
db_config = {
    'user': 'trading_user',
    'password': 'Str0ngP@ssw0rd!',
    'host': 'localhost',
    'database': 'trading_data'
}

# Trading configuration
trading_config = {
    "initial_price": Decimal('3400'),
    "offset": Decimal('10'),
    "levels_count": 10,
    "volume": 10,
    "precision": 4,
    "side": "Buy",
    "pair": "USDT_ETH",
    "take_profit": Decimal('10'),
    "commission": Decimal('0.001'),  # Commission rate, e.g., 0.1%
    "max_positions": 100,  # Maximum number of open positions
    "hedge_put_strike_1": Decimal('3400'),  # Strike price of the bought put
    "hedge_put_strike_2": Decimal('3200'),  # Strike price of the sold put
    "hedge_volume": 10  # Volume for hedging
}

def generate_adaptive_grid(config, current_price, volatility):
    grid = []
    for i in range(config['levels_count']):
        offset = Decimal(volatility) * config['offset']
        price = current_price + Decimal(i - config['levels_count'] // 2) * offset
        grid.append(round(price, config['precision']))
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

def calculate_historical_volatility(prices, window=30):
    log_returns = np.log(prices / prices.shift(1))
    volatility = log_returns.rolling(window=window).std() * np.sqrt(window)
    return volatility.iloc[-1]  # Return the most recent volatility

def get_historical_prices():
    conn = mysql.connector.connect(**db_config)
    query = "SELECT bid FROM eth_prices ORDER BY timestamp DESC LIMIT 1000"  # Example: last 1000 records
    df = pd.read_sql(query, conn)
    conn.close()
    return df['bid']

def hedge_with_back_put_spread(price, volume, config):
    bought_put_cost = price - config['hedge_put_strike_1']  # Simplified cost calculation
    sold_put_revenue = config['hedge_put_strike_2'] - price  # Simplified revenue calculation
    net_cost = bought_put_cost - sold_put_revenue
    return net_cost * volume  # Total cost of the hedge

def run_adaptive_grid_trading():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Get historical prices and calculate volatility
    historical_prices = get_historical_prices()
    volatility = calculate_historical_volatility(historical_prices)
    print(f"Calculated historical volatility: {volatility}")

    # Initial market conditions
    cursor.execute("SELECT * FROM eth_prices ORDER BY id DESC LIMIT 1")
    last_record = cursor.fetchone()
    current_price = Decimal(last_record[2])  # Assuming bid price as current price

    positions = []
    realized_profit = Decimal('0')
    unrealized_profit = Decimal('0')
    closed_trades_count = 0
    grid_recalculation_count = 0
    max_drawdown = Decimal('0')
    hedge_costs = Decimal('0')

    # Get all records
    cursor.execute("SELECT * FROM eth_prices ORDER BY id")

    for record in cursor:
        id, timestamp, bid, bid_size, ask, ask_size = record
        bid = Decimal(bid)
        ask = Decimal(ask)
        
        if id % 1000 == 0:
            print(f"\nProcessing record ID: {id}, Time: {timestamp}")
            print(f"Current price - Bid: {bid}, Ask: {ask}")

        # Generate adaptive grid based on current price and volatility
        grid = generate_adaptive_grid(trading_config, bid, volatility)
        grid_recalculation_count += 1

        # Check for buy opportunities for new positions if we haven't reached the maximum number of positions
        if len(positions) < trading_config['max_positions']:
            for level in grid:
                if ask <= level and not any(pos[0] == level for pos in positions):
                    volume = trading_config['volume'] / level
                    buy_position = execute_buy(level, volume, trading_config['commission'])
                    positions.append(buy_position)
                    if len(positions) >= trading_config['max_positions']:
                        break  # Stop adding new positions if the maximum is reached

                    # Hedge with back put spread
                    hedge_cost = hedge_with_back_put_spread(level, trading_config['hedge_volume'], trading_config)
                    hedge_costs += hedge_cost

        # Check for sell opportunities for all positions
        positions_to_remove = []
        for i, (buy_price, volume, cost_with_commission) in enumerate(positions):
            sell_price = buy_price + trading_config['take_profit']
            if bid >= sell_price:
                profit = execute_sell(buy_price, sell_price, volume, trading_config['commission'])
                realized_profit += profit
                closed_trades_count += 1
                positions_to_remove.append(i)

        # Remove sold positions
        for i in sorted(positions_to_remove, reverse=True):
            positions.pop(i)

        # Calculate unrealized P/L
        unrealized_profit = calculate_unrealized_pl(positions, bid)

        # Track the maximum drawdown
        total_invested_capital = sum(cost_with_commission for _, _, cost_with_commission in positions)
        drawdown = total_invested_capital + unrealized_profit
        if drawdown < max_drawdown:
            max_drawdown = drawdown

        # Print open positions as a table
        positions_df = pd.DataFrame(positions, columns=['Buy Price', 'Volume', 'Cost with Commission'])
        
        if id % 1000 == 0:
            print("\nCurrent open positions:")
            print(positions_df)
            print(f"Cumulative Realized P/L: {realized_profit}")
            print(f"Current Unrealized P/L: {unrealized_profit}")
            print(f"Total P/L (Realized + Unrealized): {realized_profit + unrealized_profit}")

    # Calculate final statistics
    total_invested_capital = sum(cost_with_commission for _, _, cost_with_commission in positions)

    print("\nFinal Results:")
    print(f"Total Realized Profit: {realized_profit}")
    print(f"Final Unrealized Profit: {unrealized_profit}")
    print(f"Total P/L (Realized + Unrealized): {realized_profit + unrealized_profit}")
    print(f"Remaining open positions: {len(positions)}")
    print(f"Total closed trades: {closed_trades_count}")
    print(f"Number of grid recalculations: {grid_recalculation_count}")
    print(f"Total invested capital: {total_invested_capital}")
    print(f"Maximum drawdown (excluding realized profit): {max_drawdown}")
    print(f"Total hedge costs: {hedge_costs}")

    cursor.close()
    conn.close()

# Run the adaptive grid trading simulation
run_adaptive_grid_trading()
