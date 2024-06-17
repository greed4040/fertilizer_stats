import json

with open('order_levels_generation_settings.json', 'r') as config_file:
    config = json.load(config_file)
# Извлечение параметров из конфигурационного файла
initial_price = config['initial_price']
offset = config['offset']
levels_count = config['levels_count']
volume = config['volume']
precision = config['precision']
eth_per_contract = config['eth_per_contract']
side = config["side"]
pair = config["pair"]
exchange = config["exchange"]
take_profit = config["take_profit"]

# Генерация уровней
levels = []
for i in range(levels_count):
    level_price = initial_price + (i * offset)
    level = {
        "level_number": i + 1,
        "price": level_price,
        "volume": round(volume/initial_price, precision),
        "take_profit": level_price + take_profit,
        "side": side,
        "pair": pair,
        "exchange": exchange,
        "level_name": f"{exchange}_{pair}_level_{i + 1}"
    }
    levels.append(level)

# Сохранение в конфигурационный файл
config = {
    "levels": levels
}

with open('order_levels_config.json', 'w') as config_file:
    json.dump(config, config_file, indent=4)

print("Конфигурационный файл успешно создан: order_levels_config.json")
