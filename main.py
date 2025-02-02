import json
from schedule_to_sql import download_and_process_schedule

CONFIG_PATH = '/app/config/config.json'  # Путь к конфигурационному файлу

def load_config():
    """Загружает конфигурацию из JSON-файла"""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Ошибка загрузки конфига: {e}")
        exit()

# Загружаем конфигурацию
config = load_config()

# Получаем ссылку на CSV файл из конфига
csv_url = config.get('CSV_URL')

if not csv_url:
    print("CSV_URL не найден в конфигурационном файле!")
    exit()

# Вызываем функцию для обработки расписания
download_and_process_schedule(csv_url)