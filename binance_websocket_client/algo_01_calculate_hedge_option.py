import json

# Чтение конфигурационного файла
with open('order_levels_config.json', 'r') as config_file:
    config = json.load(config_file)

# Извлечение параметров из конфигурационного файла
levels = config['levels']

# Генерация уровней цен открытия позиций и объемов сделок
prices = [level['price'] for level in levels]
volumes = [level['volume'] for level in levels]

# Общий объем купленного эфира
total_volume = sum(volumes)

# Предположим, что для учета премии мы выбрали страйк на уровне 3010 и дата истечения опциона через 1 месяц
optimal_put_option_level = 3010
expiration_date = "2024-07-14"  # Предположим, текущая дата - 2024-06-14

print(f"Рекомендуемый уровень для открытия put опциона: {optimal_put_option_level} USD")
print(f"Необходимое количество пут-опционов для хеджирования: {total_volume:.8f} ETH")
print(f"Дата истечения опциона: {expiration_date}")