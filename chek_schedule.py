#Моё расписание
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
    bot.send_message(message.chat.id, "На сколько дней вперед вывести твоё расписание? 🗓", reply_markup=markup)
    user_context[message.chat.id] = {'command': 'my_schedule'}

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

        username = f"@{message.from_user.username}" if message.from_user.username else None

        if not username:
            bot.send_message(message.chat.id, "У вас нет никнейма в Telegram. Расписание не может быть найдено.")
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
            bot.send_message(message.chat.id, "я пока умею работать только в пределах текущего месяца, сорри")

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
            bot.send_message(message.chat.id, "Тебя нет в расписании, старина")
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
                bot.send_message(message.chat.id, "Тебя нет в расписании, старина")
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
