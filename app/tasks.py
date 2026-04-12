# -*- coding: utf-8 -*-
import asyncio
import logging
from app import bot
from app.sheets import sheets_get_menu, sheets_get_all_client_ids
from app.utils import msk_now_dt, now_msk_str, seconds_until_next_930_msk, format_menu_for_broadcast
from app.services import process_auto_orders


async def daily_930_broadcast_task():
    """
    Бесконечный цикл:
      - ждём до ближайших 09:30 МСК,
      - если в Меню есть позиции на сегодня — отправляем автопостинг всем клиентам,
      - повторяем.
    """
    await asyncio.sleep(3)
    while True:
        try:
            delay = seconds_until_next_930_msk()
            await asyncio.sleep(delay)
            today_marker = (msk_now_dt()).strftime("%Y-%m-%d %H:%M:%S")
            menu_items = await sheets_get_menu(today_marker)
            if not menu_items:
                continue
            menu_text = format_menu_for_broadcast(menu_items)
            if not menu_text.strip():
                continue
            templates = [
                "В меню:\n{menu}\n\nКоличество ограничено 😋",
                "Сегодня в меню:\n{menu}\n\nУспей заказать. Количество ограничено 😋",
                "Сегодня у нас в меню:\n{menu}\n\nУспей заказать 😋",
            ]
            import random
            text_to_send = random.choice(templates).format(menu=menu_text)
            ids = await sheets_get_all_client_ids()
            if not ids:
                continue
            for uid in ids:
                try:
                    await bot.send_message(uid, text_to_send)
                except Exception:
                    pass
                await asyncio.sleep(0.05)
        except Exception:
            await asyncio.sleep(5)


async def daily_auto_order_task():
    """
    Бесконечный цикл:
      - по будням после 09:31 МСК обрабатываем автозаказы подписок,
      - повторяем.
    """
    await asyncio.sleep(5)
    while True:
        try:
            # ждём то же окно, что и рассылка меню, но +60 секунд
            delay = max(0, seconds_until_next_930_msk() + 60)
            await asyncio.sleep(delay)
            # Пн-Пт
            if msk_now_dt().weekday() >= 5:
                continue
            await process_auto_orders()
        except Exception:
            await asyncio.sleep(5)
