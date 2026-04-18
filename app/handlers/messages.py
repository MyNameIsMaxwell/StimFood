# -*- coding: utf-8 -*-
import re
from aiogram import Router, F
from aiogram.types import Message, ContentType, ReplyKeyboardRemove
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import fsm, bot
from app.config import ADMIN_IDS, is_admin
from app.keyboards import kb_send_contact, kb_choose_time, kb_sub_choose_time
from app.services import send_today_menu, h, _show_confirm
from app.sheets import sheets_add_client
from app.utils import normalize_phone

router = Router()


@router.message(F.content_type == ContentType.TEXT)
async def text_handler(message: Message):
    user = message.from_user
    if not user:
        return
    uid = user.id
    state = await fsm.get_state(uid)

    # --- Poll creation flow (admin only) ---
    if is_admin(uid) and state == "poll_awaiting_question":
        question = (message.text or "").strip()
        if len(question) < 3:
            return await message.answer("Вопрос слишком короткий. Введите вопрос длиннее.")
        await fsm.update_data(uid, poll_question=question)
        await fsm.set_state(uid, "poll_awaiting_options")
        await message.answer("Теперь отправьте варианты ответов, каждый на новой строке (минимум 2, максимум 10).")
        return

    if is_admin(uid) and state == "poll_awaiting_options":
        raw = (message.text or "").strip()
        options = [o.strip() for o in raw.splitlines() if o.strip()]
        if len(options) < 2 or len(options) > 10:
            return await message.answer("Нужно от 2 до 10 вариантов. Отправьте список ещё раз.")
        data = await fsm.get_data(uid)
        question = data.get("poll_question", "")
        await fsm.update_data(uid, poll_options=options)
        kb = InlineKeyboardBuilder()
        kb.button(text="📨 Отправить опрос всем", callback_data="poll_send")
        kb.button(text="✖️ Отменить", callback_data="poll_cancel")
        kb.adjust(1, 1)
        preview = (
            "<b>Превью опроса</b> (можно выбрать несколько вариантов):\n\n"
            + h(question)
            + "\n\n"
            + "\n".join(f"• {h(o)}" for o in options)
        )
        await message.answer(preview, reply_markup=kb.as_markup(), parse_mode="HTML")
        return

    # --- Support: user sends a message to operators ---
    if state == "awaiting_support_message":
        kb = InlineKeyboardBuilder()
        kb.button(text="Ответить", callback_data=f"support_reply:{uid}")
        admin_text = (
            f"📬 <b>Новое сообщение от пользователя</b>\n"
            f"ID: <code>{uid}</code>\n"
            f"Username: @{user.username or '-'}\n"
            f"Имя: {h(user.full_name or '-')}\n\n"
            f"{h(message.text)}"
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, admin_text, reply_markup=kb.as_markup(), parse_mode="HTML")
            except Exception:
                pass
        await message.answer("Спасибо! Сообщение передано оператору.")
        await fsm.set_state(uid, "menu")
        return

    # --- Support: admin replies to a user ---
    if is_admin(uid):
        admin_state = await fsm.get_state(uid)
        if admin_state == "awaiting_support_reply":
            data_admin = await fsm.get_data(uid)
            target = data_admin.get("reply_target")
            if target:
                try:
                    await bot.send_message(
                        target,
                        f"📩 <b>Ответ от тех.поддержки</b>:\n{h(message.text)}",
                        parse_mode="HTML"
                    )
                    await message.answer("Ответ отправлен пользователю ✅")
                except Exception:
                    await message.answer("Не удалось отправить ответ пользователю. Возможно, он ещё не начинал диалог с ботом.")
            await fsm.set_state(uid, None)
            return

    # --- Registration: awaiting name ---
    if state == "awaiting_name":
        name = (message.text or "").strip()
        if len(name) < 2:
            await message.answer("Имя должно быть длиннее. Попробуй снова, пожалуйста.")
            return
        if not re.fullmatch(r"[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)?\s+[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)?", name):
            await message.answer("Пожалуйста, введи имя и фамилию двумя словами (например: Иван Иванов).")
            return
        await fsm.update_data(uid, name=name)
        await fsm.set_state(uid, "awaiting_phone")
        kb = kb_send_contact()
        await message.answer("Отлично! Теперь отправь, пожалуйста, номер телефона (или нажми кнопку).", reply_markup=kb)
        return

    # --- Registration: awaiting phone (text) ---
    if state == "awaiting_phone":
        phone = normalize_phone(message.text or "")
        if not phone:
            await message.answer("Не могу распознать номер. Пришли его в формате +375XXXXXXXXX или нажми кнопку ниже.")
            await message.answer("Кнопка для отправки контакта:", reply_markup=kb_send_contact())
            return
        data = await fsm.get_data(uid)
        name = data.get("name", "") or (user.full_name or "")
        username = data.get("username", "") or (user.username or "")
        await sheets_add_client(uid, name, username, phone)
        await fsm.set_state(uid, "menu")
        await fsm.update_data(uid, phone=phone)
        await message.answer("Спасибо! Регистрация завершена ✅", reply_markup=ReplyKeyboardRemove())
        await send_today_menu(message.chat.id, uid)
        return

    # --- Custom address input (regular order) ---
    if state == "awaiting_custom_address":
        addr = message.text.strip()
        if len(addr) < 5:
            await message.answer("Слишком короткий адрес. Введи адрес подробнее, пожалуйста.")
            return
        await fsm.update_data(uid, chosen_address=addr)
        await fsm.set_state(uid, "choose_time")
        kb = kb_choose_time().as_markup()
        await message.answer(f"Адрес доставки: <b>{h(addr)}</b>\n\nТеперь выбери время доставки:", reply_markup=kb, parse_mode="HTML")
        return

    # --- Custom address input (subscription) ---
    if state == "sub_awaiting_custom_address":
        addr = message.text.strip()
        if len(addr) < 5:
            await message.answer("Слишком короткий адрес. Введи адрес подробнее, пожалуйста.")
            return
        await fsm.update_data(uid, chosen_address=addr)
        await fsm.set_state(uid, "sub_choose_time")
        data = await fsm.get_data(uid)
        tariff = data.get("chosen_tariff", "")
        kb = kb_sub_choose_time().as_markup()
        await message.answer(
            f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\nАдрес: <b>{h(addr)}</b>\n\nВыбери время доставки:",
            reply_markup=kb, parse_mode="HTML"
        )
        return

    # --- Manual quantity input ---
    if state == "awaiting_qty_manual":
        raw = (message.text or "").strip()
        if not re.fullmatch(r"\d{1,3}", raw) or int(raw) < 1:
            await message.answer("Нужно число ≥ 1. Попробуй ещё раз.")
            return
        qty = int(raw)
        await fsm.update_data(uid, qty=qty)
        await _show_confirm(uid, message)
        return

    # --- Default fallback ---
    if state in (None, "menu", "choose_address", "choose_time", "confirm"):
        await message.answer("Воспользуйся, пожалуйста, кнопками ниже 🙂")


@router.message(F.content_type == ContentType.CONTACT)
async def contact_handler(message: Message):
    user = message.from_user
    if not user:
        return
    uid = user.id
    state = await fsm.get_state(uid)
    if state != "awaiting_phone":
        await message.answer("Контакт получен, но сейчас он не требуется.")
        return
    phone_raw = message.contact.phone_number
    phone = normalize_phone(phone_raw) or phone_raw
    data = await fsm.get_data(uid)
    name = data.get("name", "") or (user.full_name or "")
    username = data.get("username", "") or (user.username or "")
    await sheets_add_client(uid, name, username, phone)
    await fsm.set_state(uid, "menu")
    await fsm.update_data(uid, phone=phone)
    await message.answer("Спасибо! Регистрация завершена ✅", reply_markup=ReplyKeyboardRemove())
    await send_today_menu(message.chat.id, uid)
