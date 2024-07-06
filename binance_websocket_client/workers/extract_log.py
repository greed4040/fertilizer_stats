def write_tail_to_file(input_file, output_file, num_chars=20000):
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            f.seek(0, 2)  # Перейти к концу файла
            file_size = f.tell()  # Получить размер файла
            
            # Определить начальную позицию для чтения последних num_chars символов
            start_pos = max(0, file_size - num_chars)
            
            f.seek(start_pos)
            tail_content = f.read()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(tail_content)
        
        print(f"Последние {num_chars} символов успешно записаны в {output_file}.")
    
    except Exception as e:
        print(f"Произошла ошибка: {e}")

# Путь к входному и выходному файлам
input_file = 'log_messages_result_0.log'
output_file = 'tail_log_messages_result_0.log'

# Запуск функции
write_tail_to_file(input_file, output_file)

