import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

# Инициализация
TOKEN = ''
bot = Bot(token=TOKEN)
dp = Dispatcher()

# === ВАШИ НАСТРОЙКИ ===
# Замените на ВАШ цифровой ID (без кавычек!). 
ADMIN_IDS =[814548047] 

# Замените на ID пользователей, которые УЖЕ запускали вашего бота
ALL_USERS_IDS =[8584558625, 814548047] 


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Привет! Напиши команду /poll, чтобы получить опрос.")


@dp.message(Command("poll"))
async def cmd_poll(message: types.Message):
    # Отправка опроса пользователю
    await message.answer_poll(
        question="Какой твой любимый язык программирования?",
        options=["Python", "JavaScript", "C++", "Java", "Другой"],
        is_anonymous=False,           
        allows_multiple_answers=False 
    )


# === ИСПРАВЛЕНИЕ 1: Обработка голосов ===
# Передаем bot: Bot в аргументы, чтобы бот мог писать в чат
@dp.poll_answer()
async def handle_poll_answer(poll_answer: types.PollAnswer, bot: Bot):
    user_id = poll_answer.user.id
    username = poll_answer.user.username
    selected_options = poll_answer.option_ids 
    
    # 1. Печатаем себе в консоль (для статистики)
    print(f"Пользователь @{username} проголосовал! Вариант: {selected_options}")
    
    # 2. Отвечаем пользователю ПРЯМО В ЧАТ (раскомментировано)
    await bot.send_message(
        chat_id=user_id,
        text=f"Спасибо за ваш голос! Мы его записали (выбранный индекс: {selected_options[0]})."
    )


# === ИСПРАВЛЕНИЕ 2: Рассылка от админа с отчетом ===
@dp.message(Command("send_poll_to_all"))
async def broadcast_poll(message: types.Message, bot: Bot):
    # Проверка на админа
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет прав для этой команды.")
        return

    await message.answer("Начинаю рассылку опроса...")
    
    success_count = 0
    fail_count = 0
    
    for user_id in ALL_USERS_IDS:
        try:
            await bot.send_poll(
                chat_id=user_id,
                question="Опрос от админа: Как вам этот бот?",
                options=["Супер", "Нормально", "Нужны доработки"],
                is_anonymous=False
            )
            success_count += 1
            await asyncio.sleep(0.05) # Защита от спам-лимитов Telegram
            
        except Exception as e:
            # Теперь мы видим причину ошибки прямо в консоли!
            print(f"Ошибка при отправке пользователю {user_id}: {e}")
            fail_count += 1
            
    # Подробный отчет админу прямо в бот
    await message.answer(
        f"✅ Рассылка завершена!\n\n"
        f"Успешно отправлено: {success_count} чел.\n"
        f"Ошибок (заблокировали бота/не запускали): {fail_count} чел."
    )


async def main():
    print("Бот запущен! Жду сообщений...")
    # drop_pending_updates=True очистит старые сообщения, если бот был выключен
    # allowed_updates явно указывает Telegram присылать нам результаты опросов (poll_answer)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == "__main__":
    asyncio.run(main())