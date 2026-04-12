# -*- coding: utf-8 -*-
import asyncio
import logging
from aiogram.types import BotCommand, MenuButtonCommands

from app import bot, dp, fsm
from app.handlers import register_all_handlers
from app.sheets import get_sheets_client
from app.tasks import daily_930_broadcast_task
from app.polls import get_poll_store


async def on_startup():
    await fsm.init()
    await asyncio.to_thread(get_sheets_client)
    # Init poll tables
    await get_poll_store().init_tables()
    await bot.set_my_commands([
        BotCommand(command="start", description="Начать / регистрация"),
        BotCommand(command="menu", description="Показать меню на сегодня"),
        BotCommand(command="support", description="Связаться с нами"),
        BotCommand(command="info", description="Инфо о доставке и оплате"),
    ])
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception:
        pass


async def main():
    logging.info("Starting bot...")
    register_all_handlers(dp)
    asyncio.create_task(daily_930_broadcast_task())
    await on_startup()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "poll_answer"])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")

