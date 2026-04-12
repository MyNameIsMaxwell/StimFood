# -*- coding: utf-8 -*-
from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import fsm, bot
from app.keyboards import (
    kb_choose_address, kb_choose_time, kb_choose_qty,
    kb_sub_choose_tariff, kb_sub_choose_address, kb_sub_choose_time, kb_sub_choose_days,
)
from app.services import (
    show_menu_item,
    send_today_menu,
    format_selection_for_tariff,
    edit_to_text,
    _show_sub_confirm,
)
from app.sheets import (
    sheets_find_client,
    sheets_find_menu_row,
    sheets_get_quantity_by_row,
    sheets_get_menu,
    sheets_get_week_menu,
    sheets_append_overorder,
    sheets_get_user_active_sub,
    sheets_get_user_sub_history,
)
from app.utils import h, now_msk_str, extract_ddmmyyyy, weekday_ru_from_ddmmyyyy, start_of_week_msk_str

router = Router()


@router.callback_query(F.data == "menu_prev")
async def cb_menu_prev(call: CallbackQuery):
    uid = call.from_user.id
    data = await fsm.get_data(uid)
    menu = data.get("menu", [])
    if not menu:
        return await call.answer("Меню пустое.")
    if len(menu) == 1:
        return await call.answer("Сегодня только одно блюдо 🙂")
    idx = (data.get("menu_idx", 0) - 1) % len(menu)
    await fsm.update_data(uid, menu_idx=idx)
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "menu_next")
async def cb_menu_next(call: CallbackQuery):
    uid = call.from_user.id
    data = await fsm.get_data(uid)
    menu = data.get("menu", [])
    if not menu:
        return await call.answer("Меню пустое.")
    if len(menu) == 1:
        return await call.answer("Сегодня только одно блюдо 🙂")
    idx = (data.get("menu_idx", 0) + 1) % len(menu)
    await fsm.update_data(uid, menu_idx=idx)
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data.startswith("tariff:"))
async def cb_choose_tariff(call: CallbackQuery):
    uid = call.from_user.id
    data = await fsm.get_data(uid)
    menu = data.get("menu", [])
    if not menu:
        return await call.answer("Меню пустое.")
    idx = data.get("menu_idx", 0)
    dish = str(menu[idx].get("Блюда", "")).strip()
    day = now_msk_str()
    row_index, _ = await sheets_find_menu_row(day, dish)
    if not row_index:
        return await call.answer("Позиция меню не найдена.", show_alert=True)
    qty = await sheets_get_quantity_by_row(row_index)
    if qty <= 0:
        client = await sheets_find_client(uid)
        if client:
            name = str(client.get("Имя", "")).strip()
            phone = str(client.get("Номер телефона", "")).strip()
            await sheets_append_overorder(uid, name, phone, dish)
        return await call.answer("Увы, это блюдо уже закончилось.", show_alert=True)
    code = call.data.split("tariff:", 1)[1]
    mapping = {"max": "Максимум", "standard_plus": "Стандарт+", "standard_soup": "Стандарт (+суп)", "standard_salad": "Стандарт (+салат)"}
    tariff = mapping.get(code, code)
    await fsm.update_data(uid, chosen_dish=dish, chosen_tariff=tariff)
    await fsm.set_state(uid, "choose_address")
    picked = format_selection_for_tariff(dish, tariff)
    kb = kb_choose_address().as_markup()
    text = f"Вы выбрали:\n<b>{h(picked)}</b>\nТариф: <b>{h(tariff)}</b>\n\nВыбери адрес доставки:"
    try:
        await bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    await bot.send_message(call.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data.startswith("addr:"))
async def cb_choose_address(call: CallbackQuery):
    user = call.from_user
    if not user:
        return
    uid = user.id
    address = call.data.split("addr:", 1)[1]
    await fsm.update_data(uid, chosen_address=address)
    await fsm.set_state(uid, "choose_time")
    kb = kb_choose_time().as_markup()
    text = f"Адрес доставки: <b>{h(address)}</b>\n\nТеперь выбери время доставки:"
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data.startswith("time:"))
async def cb_choose_time(call: CallbackQuery):
    user = call.from_user
    if not user:
        return
    uid = user.id
    timeslot = call.data.split("time:", 1)[1]
    await fsm.update_data(uid, chosen_time=timeslot)
    await fsm.set_state(uid, "choose_qty")
    text = (
        f"Выбери количество порций для времени <b>{h(timeslot)}</b>:\n"
        "Можно выбрать 1, 2 или ввести своё количество."
    )
    try:
        await call.message.edit_text(text, reply_markup=kb_choose_qty().as_markup(), parse_mode="HTML")
    except TelegramBadRequest:
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, text, reply_markup=kb_choose_qty().as_markup(), parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data == "confirm_cash")
async def cb_confirm_cash(call: CallbackQuery):
    from app.services import _finalize_order
    await _finalize_order(call, payment_label="оплата при получении")


@router.callback_query(F.data == "confirm_paid")
async def cb_confirm_paid(call: CallbackQuery):
    from app.services import _finalize_order
    await _finalize_order(call, payment_label="оплачено онлайн")


@router.callback_query(F.data.startswith("back:"))
async def cb_back(call: CallbackQuery):
    uid = call.from_user.id
    target = call.data.split("back:", 1)[1]
    if target in ("menu", "root"):
        await fsm.set_state(uid, "menu")
        await show_menu_item(call.message.chat.id, uid, callback_query=call)
        return
    if target == "addr":
        await fsm.set_state(uid, "choose_address")
        kb = kb_choose_address().as_markup()
        data = await fsm.get_data(uid)
        dish = data.get("chosen_dish", "")
        text = f"Выбранное блюдо: \n<b>{dish}</b>\n\nВыбери адрес доставки:"
        msg = call.message
        try:
            if msg.photo:
                await msg.edit_caption(text, reply_markup=kb, parse_mode="HTML")
            else:
                await msg.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            try:
                await bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
            await bot.send_message(msg.chat.id, text, reply_markup=kb, parse_mode="HTML")
        await call.answer()
        return
    if target == "time":
        await fsm.set_state(uid, "choose_time")
        kb = kb_choose_time().as_markup()
        data = await fsm.get_data(uid)
        address = data.get("chosen_address", "")
        text = f"Адрес доставки: <b>{address}</b>\n\nТеперь выбери время доставки:"
        msg = call.message
        try:
            if msg.photo:
                await msg.edit_caption(text, reply_markup=kb, parse_mode="HTML")
            else:
                await msg.edit_text(text, reply_markup=kb)
        except TelegramBadRequest:
            try:
                await bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
            await bot.send_message(msg.chat.id, text, reply_markup=kb, parse_mode="HTML")
        await call.answer()
        return
    if target == "qty":
        await fsm.set_state(uid, "choose_qty")
        data = await fsm.get_data(uid)
        timeslot = data.get("chosen_time", "")
        text = f"Выбери количество порций для времени <b>{h(timeslot)}</b>:"
        try:
            await call.message.edit_text(text, reply_markup=kb_choose_qty().as_markup(), parse_mode="HTML")
        except TelegramBadRequest:
            await bot.send_message(call.message.chat.id, text, reply_markup=kb_choose_qty().as_markup(), parse_mode="HTML")
        await call.answer()
        return
    await fsm.set_state(uid, "menu")
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "show_menu_again")
async def cb_show_menu_again(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "menu")
    today = now_msk_str()
    fresh_menu = await sheets_get_menu(today)
    await fsm.update_data(uid, menu=fresh_menu, menu_idx=0)
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "menu_show_week")
async def cb_menu_show_week(call: CallbackQuery):
    start = start_of_week_msk_str()
    items = await sheets_get_week_menu(start, days=7)
    if not items:
        await call.answer("Меню на неделю отсутствует", show_alert=True)
        return
    lines = []
    for it in items:
        day = extract_ddmmyyyy(str(it.get("День", "")))
        wd = weekday_ru_from_ddmmyyyy(day)
        dish = str(it.get("Блюда", "")).strip()
        lines.append(f"<b>{wd}:</b> \n {h(dish)}")
    text = "<b>Меню на неделю</b>:\n" + "\n\n".join(lines)
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад к сегодняшнему меню", callback_data="show_menu_again")
    await edit_to_text(call.message, text, reply_markup=kb.as_markup())
    await call.answer()


@router.callback_query(F.data == "addr_custom")
async def cb_addr_custom(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "awaiting_custom_address")
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="back:addr")
    kb = kb.as_markup()
    try:
        await call.message.edit_text("Пожалуйста, введи адрес доставки текстом одним сообщением.", reply_markup=kb)
    except TelegramBadRequest:
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, "Пожалуйста, введи адрес доставки текстом одним сообщением.", reply_markup=kb)
    await call.answer()


@router.callback_query(F.data.startswith("support_reply:"))
async def cb_support_reply(call: CallbackQuery):
    from app.config import is_admin
    admin_uid = call.from_user.id
    if not is_admin(admin_uid):
        return await call.answer("Нет прав", show_alert=True)
    target_user_id = int(call.data.split(":", 1)[1])
    await fsm.set_state(admin_uid, "awaiting_support_reply")
    await fsm.update_data(admin_uid, reply_target=target_user_id)
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.answer(
        f"Напишите ответ для пользователя <code>{target_user_id}</code> одним сообщением.",
        parse_mode="HTML"
    )
    await call.answer()


@router.callback_query(F.data.startswith("qty:"))
async def cb_choose_qty(call: CallbackQuery):
    uid = call.from_user.id
    choice = call.data.split("qty:", 1)[1]
    if choice == "more":
        await fsm.set_state(uid, "awaiting_qty_manual")
        await call.message.edit_text("Введи количество порций числом (например, 3).", reply_markup=None, parse_mode="HTML")
        await call.answer()
        return
    qty = int(choice)
    await fsm.update_data(uid, qty=qty)
    from app.services import _show_confirm
    await _show_confirm(uid, call.message)
    await call.answer()


# --------- SUBSCRIPTION STATUS / HISTORY ---------

@router.callback_query(F.data == "sub:status")
async def cb_sub_status(call: CallbackQuery):
    uid = call.from_user.id
    sub = await sheets_get_user_active_sub(uid)
    if not sub:
        await call.answer("У тебя нет активной подписки.", show_alert=True)
        return

    tariff = sub.get("Тариф", "—")
    address = sub.get("Адрес", "—")
    timeslot = sub.get("Время", "—")
    days_total = sub.get("Дней", "—")
    days_left = sub.get("Осталось дней", "—")
    date_created = sub.get("Дата оформления", "—")
    total_price = sub.get("Стоимость", "—")
    payment = sub.get("Оплата", "—")

    text = (
        "📋 <b>Активная подписка</b>\n\n"
        f"• Тариф: <b>{h(tariff)}</b>\n"
        f"• Адрес: <b>{h(address)}</b>\n"
        f"• Время: <b>{h(timeslot)}</b>\n"
        f"• Длительность: <b>{h(str(days_total))} дн.</b>\n"
        f"• Осталось: <b>{h(str(days_left))} дн.</b>\n"
        f"• Стоимость: <b>{h(str(total_price))} р</b>\n"
        f"• Оплата: {h(payment)}\n"
        f"• Дата оформления: {h(date_created)}"
    )

    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📜 История подписок", callback_data="sub:history"))
    kb.row(InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="show_menu_again"))

    try:
        await bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    await bot.send_message(call.message.chat.id, text, reply_markup=kb.as_markup(), parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data == "sub:history")
async def cb_sub_history(call: CallbackQuery):
    uid = call.from_user.id
    history = await sheets_get_user_sub_history(uid)

    if not history:
        await call.answer("История подписок пуста.", show_alert=True)
        return

    lines = []
    for i, sub in enumerate(history[:10], 1):  # макс. 10 последних
        status = sub.get("Статус", "—")
        tariff = sub.get("Тариф", "—")
        days_total = sub.get("Дней", "—")
        days_left = sub.get("Осталось дней", "—")
        date_created = sub.get("Дата оформления", "—")
        total_price = sub.get("Стоимость", "—")

        status_icon = "✅" if "завершена" in status.lower() else ("🔥" if "активна" in status.lower() else "❌")
        lines.append(
            f"{i}. {status_icon} <b>{h(tariff)}</b> — {h(str(days_total))} дн. "
            f"(осталось {h(str(days_left))})\n"
            f"   {h(date_created)} • {h(str(total_price))} р • {h(status)}"
        )

    text = "📜 <b>История подписок</b>\n\n" + "\n\n".join(lines)

    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="⬅️ Назад к меню", callback_data="show_menu_again"))

    try:
        await call.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    except TelegramBadRequest:
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, text, reply_markup=kb.as_markup(), parse_mode="HTML")
    await call.answer()


# --------- SUBSCRIPTION FLOW ---------

TARIFF_MAPPING = {
    "max": "Максимум",
    "standard_plus": "Стандарт+",
    "standard_soup": "Стандарт (+суп)",
    "standard_salad": "Стандарт (+салат)",
}

SUB_TARIFF_INFO = (
    "\n\n<b>Тарифы:</b>\n"
    "• <b>Максимум</b> — горячее, суп, салат, напиток, фирменный соус: <b>16 р/день</b>\n"
    "• <b>Стандарт+</b>  — горячее, суп, салат: <b>14 р/день</b>\n"
    "• <b>Стандарт</b> (+суп) — горячее + суп: <b>12 р/день</b>\n"
    "• <b>Стандарт</b> (+салат) — горячее + салат: <b>12 р/день</b>"
)


@router.callback_query(F.data == "sub:start")
async def cb_sub_start(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "sub_choose_tariff")
    text = "🔥 <b>Умный заказ</b> — подписка на обеды\n\nВыбери тариф:" + SUB_TARIFF_INFO
    try:
        await bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_tariff().as_markup(), parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data.startswith("sub_tariff:"))
async def cb_sub_tariff(call: CallbackQuery):
    uid = call.from_user.id
    code = call.data.split("sub_tariff:", 1)[1]
    tariff = TARIFF_MAPPING.get(code, code)
    await fsm.update_data(uid, chosen_tariff=tariff)
    await fsm.set_state(uid, "sub_choose_address")
    text = f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\n\nВыбери адрес доставки:"
    try:
        await call.message.edit_text(text, reply_markup=kb_sub_choose_address().as_markup(), parse_mode="HTML")
    except Exception:
        await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_address().as_markup(), parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data.startswith("sub_addr:"))
async def cb_sub_addr(call: CallbackQuery):
    uid = call.from_user.id
    address = call.data.split("sub_addr:", 1)[1]
    await fsm.update_data(uid, chosen_address=address)
    await fsm.set_state(uid, "sub_choose_time")
    data = await fsm.get_data(uid)
    tariff = data.get("chosen_tariff", "")
    text = f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\nАдрес: <b>{h(address)}</b>\n\nВыбери время доставки:"
    try:
        await call.message.edit_text(text, reply_markup=kb_sub_choose_time().as_markup(), parse_mode="HTML")
    except Exception:
        await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_time().as_markup(), parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data.startswith("sub_time:"))
async def cb_sub_time(call: CallbackQuery):
    uid = call.from_user.id
    timeslot = call.data.split("sub_time:", 1)[1]
    await fsm.update_data(uid, chosen_time=timeslot)
    await fsm.set_state(uid, "sub_choose_days")
    data = await fsm.get_data(uid)
    tariff = data.get("chosen_tariff", "")
    address = data.get("chosen_address", "")
    text = (
        f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\n"
        f"Адрес: <b>{h(address)}</b>\nВремя: <b>{h(timeslot)}</b>\n\n"
        "Выбери длительность подписки:"
    )
    try:
        await call.message.edit_text(text, reply_markup=kb_sub_choose_days().as_markup(), parse_mode="HTML")
    except Exception:
        await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_days().as_markup(), parse_mode="HTML")
    await call.answer()


@router.callback_query(F.data.startswith("sub_days:"))
async def cb_sub_days(call: CallbackQuery):
    uid = call.from_user.id
    days = int(call.data.split("sub_days:", 1)[1])
    await fsm.update_data(uid, sub_days=days)
    await fsm.set_state(uid, "sub_confirm")
    await _show_sub_confirm(uid, call.message)
    await call.answer()


@router.callback_query(F.data == "sub_confirm_cash")
async def cb_sub_confirm_cash(call: CallbackQuery):
    from app.services import _finalize_subscription
    await _finalize_subscription(call, payment_label="подписка: оплата при получении")


@router.callback_query(F.data == "sub_confirm_paid")
async def cb_sub_confirm_paid(call: CallbackQuery):
    from app.services import _finalize_subscription
    await _finalize_subscription(call, payment_label="подписка: оплачено онлайн")


@router.callback_query(F.data.startswith("sub_back:"))
async def cb_sub_back(call: CallbackQuery):
    uid = call.from_user.id
    target = call.data.split("sub_back:", 1)[1]
    data = await fsm.get_data(uid)

    if target == "menu":
        await fsm.set_state(uid, "menu")
        await show_menu_item(call.message.chat.id, uid, callback_query=call)
        return

    if target == "tariff":
        await fsm.set_state(uid, "sub_choose_tariff")
        text = "🔥 <b>Умный заказ</b> — подписка на обеды\n\nВыбери тариф:" + SUB_TARIFF_INFO
        try:
            await call.message.edit_text(text, reply_markup=kb_sub_choose_tariff().as_markup(), parse_mode="HTML")
        except Exception:
            await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_tariff().as_markup(), parse_mode="HTML")
        await call.answer()
        return

    if target == "addr":
        await fsm.set_state(uid, "sub_choose_address")
        tariff = data.get("chosen_tariff", "")
        text = f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\n\nВыбери адрес доставки:"
        try:
            await call.message.edit_text(text, reply_markup=kb_sub_choose_address().as_markup(), parse_mode="HTML")
        except Exception:
            await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_address().as_markup(), parse_mode="HTML")
        await call.answer()
        return

    if target == "time":
        await fsm.set_state(uid, "sub_choose_time")
        tariff = data.get("chosen_tariff", "")
        address = data.get("chosen_address", "")
        text = f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\nАдрес: <b>{h(address)}</b>\n\nВыбери время доставки:"
        try:
            await call.message.edit_text(text, reply_markup=kb_sub_choose_time().as_markup(), parse_mode="HTML")
        except Exception:
            await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_time().as_markup(), parse_mode="HTML")
        await call.answer()
        return

    if target == "days":
        await fsm.set_state(uid, "sub_choose_days")
        tariff = data.get("chosen_tariff", "")
        address = data.get("chosen_address", "")
        timeslot = data.get("chosen_time", "")
        text = (
            f"🔥 <b>Умный заказ</b>\nТариф: <b>{h(tariff)}</b>\n"
            f"Адрес: <b>{h(address)}</b>\nВремя: <b>{h(timeslot)}</b>\n\n"
            "Выбери длительность подписки:"
        )
        try:
            await call.message.edit_text(text, reply_markup=kb_sub_choose_days().as_markup(), parse_mode="HTML")
        except Exception:
            await bot.send_message(call.message.chat.id, text, reply_markup=kb_sub_choose_days().as_markup(), parse_mode="HTML")
        await call.answer()
        return

    # fallback
    await fsm.set_state(uid, "menu")
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "sub_addr_custom")
async def cb_sub_addr_custom(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "sub_awaiting_custom_address")
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад", callback_data="sub_back:tariff")
    try:
        await call.message.edit_text(
            "Пожалуйста, введи адрес доставки для подписки одним сообщением.",
            reply_markup=kb.as_markup()
        )
    except Exception:
        await bot.send_message(
            call.message.chat.id,
            "Пожалуйста, введи адрес доставки для подписки одним сообщением.",
            reply_markup=kb.as_markup()
        )
    await call.answer()

