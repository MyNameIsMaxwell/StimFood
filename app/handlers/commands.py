# -*- coding: utf-8 -*-
from aiogram import Router
from aiogram.filters import CommandStart, Command
from aiogram.types import Message
import asyncio

from app import bot, fsm
from app.config import is_admin
from app.services import ensure_registered_and_show_menu, send_today_menu
from app.sheets import sheets_get_all_client_ids

router = Router()


@router.message(Command("menu"))
async def cmd_menu(message: Message):
    user = message.from_user
    if not user:
        return
    uid = user.id
    await fsm.set_state(uid, "menu")
    await send_today_menu(message.chat.id, uid)


@router.message(Command("support"))
async def cmd_support(message: Message):
    uid = message.from_user.id
    await fsm.set_state(uid, "awaiting_support_message")
    await message.answer(
        "Опиши, пожалуйста, вопрос одним сообщением — я перешлю его оператору.\n"
        "Либо свяжись с нами по номеру <b>+375333777308</b>.\n\n"
        "Чтобы отменить — отправь /menu."
    )


@router.message(Command("info"))
async def cmd_info(message: Message):
    await message.answer("""<b>Доставка</b> 🚚
В Катин Бор на проходную в 11.50
На Дубровскую на проходную в 12.10
На Цельсий в холе в 12.00
<b>Оплата:</b>
ЕРИП - Сервис E-POS - Ввести номер лицевого счета ( 29286-1-6599 ) - ввести ФИО и сумму
Приятного аппетита 😋""")


@router.message(Command("send_all"))
async def admin_broadcast(message: Message):
    if not is_admin(message.from_user.id):
        return await message.answer("Нет прав для рассылки.")
    text_to_send = message.text.partition(" ")[2].strip()
    if not text_to_send:
        return await message.answer("Использование: /send_all Текст оповещения")
    ids = await sheets_get_all_client_ids()
    if not ids:
        return await message.answer("В листе «Клиенты» нет получателей.")
    sent, fail = 0, 0
    for uid in ids:
        try:
            await bot.send_message(uid, text_to_send)
            sent += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    await message.answer(f"Рассылка завершена.\nУспешно: {sent}\nОшибок: {fail}")


@router.message(Command("poll"))
async def cmd_poll(message: Message):
    uid = message.from_user.id
    if not is_admin(uid):
        return await message.answer("Нет прав для этой команды.")
    await fsm.set_state(uid, "poll_awaiting_question")
    await message.answer("Введите текст вопроса для опроса.")


@router.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_registered_and_show_menu(message)
