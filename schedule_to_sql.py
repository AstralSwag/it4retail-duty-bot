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
        'май': '05',
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

# Функция для обработки команды "Моё расписание"
@bot.message_handler(func=lambda message: message.text == "Моё расписание")
def my_schedule_handler(message):
    user_info = f"User: {message.from_user.first_name} (@{message.from_user.username or 'No username'})"
    logging.info(f"{user_info} requested 'Моё расписание'")
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add(
        telebot.types.KeyboardButton("На завтра"),
        telebot.types.KeyboardButton("3️⃣"),
        telebot.types.KeyboardButton("7️⃣"),
        telebot.types.KeyboardButton("Покажи весь месяц")
    )
    bot.send_message(message.chat.id, "На сколько дней вперед вывести расписание? 🗓", reply_markup=markup)
    user_context[message.chat.id] = {'command': 'my_schedule'}

# Функция для обработки ввода количества дней и вывода расписания
@bot.message_handler(func=lambda message: message.chat.id in user_context and user_context[message.chat.id]['command'] == 'my_schedule')
def handle_schedule_days_input(message):
    user_info = f"User: {message.from_user.first_name} (@{message.from_user.username or 'No username'})"
    try:
        user_input = message.text.strip()
        if user_input == "На завтра":
            days = 2
        elif user_input == "3️⃣":
            days = 4
        elif user_input == "7️⃣":
            days = 8
        elif user_input == "Покажи весь месяц":
            days = 31  # Максимально возможное количество дней в месяце
        else:
            days = int(user_input)

        # Получаем имя пользователя из контекста или словаря HEAD_MAPPING
        # Например, можно передать имя пользователя через user_context
        if 'selected_user' in user_context.get(message.chat.id, {}):
            selected_user = user_context[message.chat.id]['selected_user']
            username = HEAD_MAPPING.get(selected_user)
        else:
            username = f"@{message.from_user.username}" if message.from_user.username else None

        if not username:
            bot.send_message(message.chat.id, "Указанный пользователь не найден в расписании.")
            logging.info(f"{user_info} - No username provided, schedule lookup failed.")
            return

        # Подключение к базе данных
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Получаем текущую дату
        today = datetime.now()
        last_day = today.replace(day=1) + timedelta(days=32)  # Переходим на следующий месяц
        last_day = last_day.replace(day=1) - timedelta(days=1)  # Последний день текущего месяца

        # Рассчитываем конечную дату вывода расписания
        end_date = today + timedelta(days=days - 1)
        if end_date > last_day:
            end_date = last_day
            bot.send_message(message.chat.id, "Я пока умею работать только в пределах текущего месяца, сорри")

        # Форматируем даты для SQL
        start_date_str = today.strftime('%d.%m.%Y')
        end_date_str = end_date.strftime('%d.%m.%Y')

        # Запрос для получения расписания пользователя
        query = f"""
        SELECT Date, Time, `{username}`
        FROM schedule
        WHERE Date BETWEEN ? AND ?
        """
        cursor.execute(query, (start_date_str, end_date_str))
        rows = cursor.fetchall()

        if not rows:
            bot.send_message(message.chat.id, f"Пользователь {selected_user} не найден в расписании.")
            logging.info(f"{user_info} - No schedule found for {username}")
        else:
            schedule = []
            for row in rows:
                status = row[username]
                if status is None:
                    continue
                formatted_status = STATUS_MAPPING.get(status, status)
                weekday = get_weekday(row['Date'])
                today_marker = " 👈 Сегодня" if row['Date'] == today.strftime('%d.%m.%Y') else ""
                if status == 'duty':
                    schedule.append(f"*{escape_markdown(row['Date'])}* \\({escape_markdown(weekday)}\\) \\- {escape_markdown(formatted_status)} {escape_markdown(row['Time'])}{escape_markdown(today_marker)}\n")
                else:
                    schedule.append(f"*{escape_markdown(row['Date'])}* \\({escape_markdown(weekday)}\\) \\- {escape_markdown(formatted_status)}{escape_markdown(today_marker)}\n")

            if schedule:
                table = "\n".join(schedule)
                bot.send_message(message.chat.id, table, parse_mode="MarkdownV2")
                logging.info(f"{user_info} - Schedule sent for {username}")
            else:
                bot.send_message(message.chat.id, f"Пользователь {selected_user} не найден в расписании.")
                logging.info(f"{user_info} - No schedule found after filtering for {username}")

        # Возврат кнопок по умолчанию
        markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
        button_duty = telebot.types.KeyboardButton("Кто дежурит?")
        button_schedule = telebot.types.KeyboardButton("Моё расписание")
        markup.add(button_duty, button_schedule)
        bot.send_message(message.chat.id, "____________", reply_markup=markup)

    except ValueError:
        bot.send_message(message.chat.id, "Пожалуйста, введите корректное число дней.")
        logging.info(f"{user_info} - Invalid input for schedule days.")
    except Exception as e:
        bot.send_message(message.chat.id, f"Произошла ошибка: {e}")
        logging.error(f"{user_info} - Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
        user_context.pop(message.chat.id, None)