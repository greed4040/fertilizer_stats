import json
import datetime

def write_tail_as_json_list_with_dates(input_file, output_file, num_chars=2000):
    def convert_unix_to_date(unix_time):
        return datetime.datetime.utcfromtimestamp(unix_time / 1000).strftime('%Y-%m-%d %H:%M:%S')

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            f.seek(0, 2)  # Перейти к концу файла
            file_size = f.tell()  # Получить размер файла
            
            # Определить начальную позицию для чтения последних num_chars символов
            start_pos = max(0, file_size - num_chars)
            
            f.seek(start_pos)
            tail_content = f.read()
        
        # Найти первый символ '{'
        first_brace_index = tail_content.find('{')
        if (first_brace_index == -1):
            raise ValueError("Символ '{' не найден в последних {} символах файла.".format(num_chars))
        
        # Извлечь текст, начиная с первого символа '{'
        json_like_content = tail_content[first_brace_index:]
        
        # Заменить '}{' на '},{' для создания валидного JSON списка
        json_like_content = json_like_content.replace('}{', '},{')
        
        # Добавить открывающий и закрывающий квадратные скобки
        json_array_content = f'[{json_like_content}]'
        
        # Проверить, что получившийся текст является валидным JSON
        try:
            json_data = json.loads(json_array_content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка декодирования JSON: {e}")
        
        # Добавить строковые варианты дат
        for item in json_data:
            if 'E' in item:
                item['E_date'] = convert_unix_to_date(item['E'])
            if 'T' in item:
                item['T_date'] = convert_unix_to_date(item['T'])
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=4)
        
        print(f"Последние {num_chars} символов успешно записаны в {output_file} в виде списка словарей с добавленными датами.")
    
    except Exception as e:
        print(f"Произошла ошибка: {e}")

# Путь к входному и выходному файлам
input_file = 'log_messages_result_0.log'
output_file = 'tail_log_messages_result_0.json'

# Запуск функции
write_tail_as_json_list_with_dates(input_file, output_file)
