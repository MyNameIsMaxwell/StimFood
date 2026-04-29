# -*- coding: utf-8 -*-
import asyncio
import logging
import random

from app import bot
from app.sheets import sheets_get_menu, sheets_get_all_client_ids
from app.utils import (
    msk_now_dt,
    seconds_until_next_930_msk,
    seconds_until_next_hhmm_msk,
    format_menu_for_broadcast,
    h,
)
from app.services import process_auto_orders

# Повтор после ошибки (рассылка / автозаказы)
RETRY_DELAY_SEC = 300
# Ограничение попыток за одно «окно» утра, чтобы не крутиться бесконечно при постоянной ошибке
MAX_BROADCAST_ATTEMPTS = 24
MAX_AUTO_ORDER_ROUNDS = 36

# Автозаказы по подпискам — отдельное время от рассылки меню (меньше конкуренции за Google Sheets API)
AUTO_ORDER_HOUR_MSK = 9
AUTO_ORDER_MINUTE_MSK = 45


async def _run_930_broadcast_once() -> None:
    """Одна попытка рассылки меню в 9:30. Пустое меню / нет клиентов — нормальный выход без исключения."""
    today_marker = (msk_now_dt()).strftime("%Y-%m-%d %H:%M:%S")
    menu_items = await sheets_get_menu(today_marker)
    if not menu_items:
        return
    menu_text = format_menu_for_broadcast(menu_items)
    if not menu_text.strip():
        return
    templates = [
        "В меню:\n{menu}\n\n<b>Попробуй новинку</b>:\n🎁<b> 5+1</b>: шестой обед в подарок при заказе от 5 тарифов в одной доставке.\n🤖 <b>«Умный заказ»</b> — подписка за 2 клика\n\nКоличество ограничено 😋",
        "Сегодня в меню:\n{menu}\n\n<b>Попробуй новинку</b>:\n🎁<b> 5+1</b>: шестой обед в подарок при заказе от 5 тарифов в одной доставке.\n🤖 <b>«Умный заказ»</b> — подписка за 2 клика\n\nУспей заказать. Количество ограничено 😋",
        "Сегодня у нас в меню:\n{menu}\n\n<b>Попробуй новинку</b>:\n🎁<b> 5+1</b>: шестой обед в подарок при заказе от 5 тарифов в одной доставке.\n🤖 <b>«Умный заказ»</b> — подписка за 2 клика\n\nУспей заказать 😋",
    ]
    text_to_send = random.choice(templates).format(menu=h(menu_text))
    ids = await sheets_get_all_client_ids()
    if not ids:
        return
    for uid in ids:
        try:
            await bot.send_message(uid, text_to_send, parse_mode="HTML")
        except Exception:
            pass
        await asyncio.sleep(0.05)


async def daily_930_broadcast_task():
    """
    Ожидает 09:30 МСК, рассылает меню. При ошибке (Sheets, сеть) — повтор через 5 минут.
    Не связана по времени с автозаказами подписок (отдельная задача).
    """
    await asyncio.sleep(3)
    while True:
        try:
            delay = seconds_until_next_930_msk()
            await asyncio.sleep(delay)
            attempt = 0
            while attempt < MAX_BROADCAST_ATTEMPTS:
                try:
                    await _run_930_broadcast_once()
                    break
                except Exception:
                    logging.exception("daily_930_broadcast_task failed")
                    attempt += 1
                    await asyncio.sleep(RETRY_DELAY_SEC)
            if attempt >= MAX_BROADCAST_ATTEMPTS:
                logging.error(
                    "daily_930_broadcast_task: исчерпаны попытки (%s) до следующего дня",
                    MAX_BROADCAST_ATTEMPTS,
                )
        except Exception:
            logging.exception("daily_930_broadcast_task outer")
            await asyncio.sleep(5)


async def daily_auto_order_task():
    """
    По будням в отдельное время (9:45 МСК) обрабатывает подписки.
    Расписание не завязано на 9:30 + смещение: своё окно + повтор неудач через 5 минут.
    """
    await asyncio.sleep(5)
    while True:
        try:
            delay = seconds_until_next_hhmm_msk(AUTO_ORDER_HOUR_MSK, AUTO_ORDER_MINUTE_MSK)
            await asyncio.sleep(delay)
            if msk_now_dt().weekday() >= 5:
                continue
            rounds = 0
            pending = None
            while rounds < MAX_AUTO_ORDER_ROUNDS:
                try:
                    failed = await process_auto_orders(pending)
                    if not failed:
                        break
                    pending = failed
                    rounds += 1
                    await asyncio.sleep(RETRY_DELAY_SEC)
                except Exception:
                    logging.exception("daily_auto_order_task failed")
                    pending = None
                    rounds += 1
                    await asyncio.sleep(RETRY_DELAY_SEC)
            if rounds >= MAX_AUTO_ORDER_ROUNDS and pending:
                logging.error(
                    "daily_auto_order_task: исчерпаны раунды повтора, остались подписки без заказа"
                )
        except Exception:
            logging.exception("daily_auto_order_task outer")
            await asyncio.sleep(5)
