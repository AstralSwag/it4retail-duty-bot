import pandas as pd
import sqlite3
import requests
from datetime import datetime
import dotenv
from dotenv import load_dotenv
import os
import json

load_dotenv()
head_mapping = json.loads(os.getenv('HEAD_MAPPING'))


def download_and_process_schedule(csv_url):

    if not csv_url:
        print("URL не найден в .env файле!")
        exit()

    # Скачиваем CSV файл
    response = requests.get(csv_url)

    # Проверяем успешность запроса
    if response.status_code == 200:
        # Сохраняем файл
        with open('schedule.csv', 'wb') as f:
            f.write(response.content)
        print("CSV файл успешно скачан и сохранен как 'schedule.csv'.")
    else:
        print(f"Ошибка при скачивании файла: {response.status_code}")
        exit()

    # Загружаем CSV-файл
    file_path = './schedule.csv'
    df = pd.read_csv(file_path)

    # Обрезаем колонки после последней, которая не является пустой
    columns_to_keep = [col for col in df.columns if 'Unnamed' not in col]
    df_filtered = df[columns_to_keep].copy()  # Создаём копию данных

    # Находим последнюю дату и удаляем строки после неё
    date_rows = df_filtered['Дата'].dropna()
    last_date_index = date_rows.last_valid_index()
    df_filtered = df_filtered.loc[:last_date_index]

    # Заполняем пропуски в столбце 'Дата' значениями из предыдущей строки
    df_filtered['Дата'] = df_filtered['Дата'].ffill()

    # Преобразуем даты в формат YYYY-MM-DD
    df_filtered['Дата'] = df_filtered['Дата'].apply(get_date)

    status_mapping = {
        'р': 'work',
        'о': 'vacation',
        '+': 'duty',
        'в': 'dayoff'
    }

    # Переименовываем столбцы на английский
    df_filtered.rename(columns=head_mapping, inplace=True)

    # Применяем замену статусов
    for col in df_filtered.columns[2:]:  # Начинаем с третьей колонки, где начинаются имена сотрудников
        df_filtered[col] = df_filtered[col].replace(status_mapping)  # Заменяем статусы с помощью replace

    # Создаём базу данных SQLite и записываем таблицу
    db_name = './schedule.db'
    table_name = 'schedule'

    conn = sqlite3.connect(db_name)
    df_filtered.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

    print("Данные успешно сохранены в базе данных.")


def get_date(rus_date):
    parts = rus_date.split(', ')
    day = parts[1].split(' ')[0]
    month_cyr = parts[1].split(' ')[1][:3]  # Учитываем только первые 3 символа

    month_mapping = {
        'янв': '01',
        'фев': '02',
        'мар': '03',
        'апр': '04',
        'мая': '05',
        'июн': '06',
        'июл': '07',
        'авг': '08',
        'сен': '09',
        'окт': '10',
        'ноя': '11',
        'дек': '12'
    }
    month = month_mapping.get(month_cyr)
    if not month:
        raise ValueError(f"Unknown month abbreviation: {month_cyr}")


    year = datetime.now().strftime('%Y')
    return f"{day.zfill(2)}.{month}.{year}"