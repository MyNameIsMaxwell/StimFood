# -*- coding: utf-8 -*-
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

from aiogram.enums import ChatAction
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InputMediaPhoto,
    Message,
)
import aiohttp

from app import bot, fsm
from app.config import NOTIFY_IDS
from app.crm import crm_send_order
from app.keyboards import (
    kb_choose_address,
    kb_choose_qty,
    kb_choose_time,
    kb_confirm,
    kb_menu_navigation,
    kb_show_menu_again,
)
from app.sheets import (
    reserve_portions_for_today,
    release_portions_for_today,
    sheets_find_client,
    sheets_find_menu_row,
    sheets_get_menu,
    sheets_get_quantity_by_row,
    sheets_get_week_menu,
    sheets_set_quantity_by_row,
    sheets_append_order,
    sheets_append_overorder,
)
from app.utils import (
    extract_ddmmyyyy,
    h,
    now_msk_str,
    parse_ddmmyyyy,
    start_of_week_msk_str,
    weekday_ru_from_ddmmyyyy,
)


def normalize_photo_url(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    m = re.search(r"drive\.google\.com/(?:file/d/|uc\?id=)([^/&#?]+)", url)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url


def _strip_header_line(text: str) -> list[str]:
    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln]
    if lines and re.match(r"^Блюдо дня[:：]?\s*$", lines[0], flags=re.IGNORECASE):
        lines = lines[1:]
    return lines


def split_menu_components(dish_text: str) -> dict:
    lines = _strip_header_line(dish_text)
    soup = lines[0] if len(lines) > 0 else None
    hot = lines[1] if len(lines) > 1 else None
    salad = lines[2] if len(lines) > 2 else None
    drink = lines[3] if len(lines) > 3 else None
    other = lines[4:] if len(lines) > 4 else []
    return {"soup": soup, "hot": hot, "salad": salad, "drink": drink, "other": other}


def format_selection_for_tariff(dish_text: str, tariff: str) -> str:
    parts = split_menu_components(dish_text)
    want = []
    t = (tariff or "").lower()
    if "максимум" in t:
        want = ["soup", "hot", "salad", "drink"]
    elif "стандарт+" in t:
        want = ["soup", "hot", "salad"]
    elif "суп" in t:
        want = ["soup", "hot"]
    elif "салат" in t:
        want = ["hot", "salad"]
    else:
        want = ["hot"]
    ordered = []
    order = ["soup", "hot", "salad", "drink"]
    for key in order:
        if key in want and parts.get(key):
            ordered.append(parts[key])
    if not ordered:
        ordered = _strip_header_line(dish_text)
    return "\n".join(ordered)


async def fetch_bytes(url: str, timeout: int = 20) -> Optional[bytes]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=timeout) as r:
                if r.status == 200:
                    return await r.read()
    except Exception:
        return None
    return None


async def _safe_send_photo_or_text(chat_id: int, photo_url: str, caption: str, reply_markup):
    url = normalize_photo_url(photo_url) if photo_url else ""
    if url:
        try:
            await bot.send_photo(chat_id, photo=url, caption=caption, reply_markup=reply_markup)
            return
        except TelegramBadRequest as e:
            if "wrong type of the web page content" not in str(e).lower():
                pass
        except Exception:
            pass
    if url:
        data = await fetch_bytes(url)
        if data:
            try:
                await bot.send_photo(
                    chat_id,
                    photo=BufferedInputFile(data, filename="dish.jpg"),
                    caption=caption,
                    reply_markup=reply_markup
                )
                return
            except Exception:
                pass
    await bot.send_message(
        chat_id,
        f"{caption}\n(Фото недоступно по ссылке)",
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )


async def edit_text_or_caption(msg, text: str, reply_markup=None, parse_mode: str | None = "HTML"):
    try:
        if getattr(msg, "photo", None):
            await msg.edit_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
        else:
            await msg.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except TelegramBadRequest:
        try:
            await msg.bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass
        await msg.bot.send_message(msg.chat.id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return False


async def _edit_media_smart(msg, photo_url: str, caption: str, kb) -> bool:
    url = normalize_photo_url(photo_url) if photo_url else ""
    if not url:
        return False
    try:
        await msg.edit_media(
            InputMediaPhoto(media=url, caption=caption),
            reply_markup=kb
        )
        return True
    except TelegramBadRequest as e:
        if "wrong type of the web page content" not in str(e).lower():
            pass
    except Exception:
        pass
    data = await fetch_bytes(url)
    if data:
        try:
            await msg.edit_media(
                InputMediaPhoto(media=BufferedInputFile(data, filename="dish.jpg"), caption=caption),
                reply_markup=kb
            )
            return True
        except Exception:
            return False
    return False


async def edit_to_text(msg: Message, text: str, reply_markup=None):
    try:
        if getattr(msg, "photo", None) or getattr(msg, "video", None) or getattr(msg, "document", None):
            try:
                await msg.bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
            await msg.bot.send_message(msg.chat.id, text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await msg.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest:
        try:
            await msg.bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass
        await msg.bot.send_message(msg.chat.id, text, reply_markup=reply_markup, parse_mode="HTML")


async def show_menu_item(
    chat_id: int,
    user_id: int,
    edit_message: Optional[Message] = None,
    callback_query: Optional[CallbackQuery] = None
):
    data = await fsm.get_data(user_id)
    menu: List[Dict[str, Any]] = data.get("menu", [])
    idx: int = data.get("menu_idx", 0)
    if not menu:
        await bot.send_message(chat_id, "Меню на сегодня пустое.")
        return
    idx = max(0, min(idx, len(menu) - 1))
    item = menu[idx]
    dish_name = str(item.get("Блюда", "")).strip() or "Без названия"
    qty = str(item.get("Количество", "")).strip()
    photo = str(item.get("Фото", "")).strip()

    caption_lines = [
        f"<b>Блюдо дня</b>:\n{h(dish_name)}",
        "",
        "<b>Тарифы:</b>",
        "• <b>Максимум</b> — горячее, суп, салат, напиток, фирменный соус: <b>15 р</b>",
        "• <b>Стандарт+</b>  — горячее, суп, салат: <b>13 р</b>",
        "• <b>Стандарт</b> (+суп) — горячее + суп: <b>11 р</b>",
        "• <b>Стандарт</b> (+салат) — горячее + салат: <b>11 р</b>",
        ""
    ]
    if qty:
        caption_lines.append(f"Доступно: {h(qty)}")
    text_or_caption = "\n".join(caption_lines)
    can_switch = len(menu) > 1
    kb = kb_menu_navigation(can_switch=can_switch, include_tariffs=True).as_markup()

    if not callback_query:
        try:
            await bot.send_chat_action(chat_id, ChatAction.UPLOAD_PHOTO if photo else ChatAction.TYPING)
        except Exception:
            pass
        if photo:
            await _safe_send_photo_or_text(chat_id, photo, text_or_caption, kb)
        else:
            await bot.send_message(chat_id, text_or_caption, reply_markup=kb, parse_mode="HTML")
        return

    msg = callback_query.message
    try:
        if photo:
            if msg.photo:
                ok = await _edit_media_smart(msg, photo, text_or_caption, kb)
                if not ok:
                    try:
                        await bot.delete_message(msg.chat.id, msg.message_id)
                    except Exception:
                        pass
                    await _safe_send_photo_or_text(chat_id, photo, text_or_caption, kb)
            else:
                try:
                    await bot.delete_message(msg.chat.id, msg.message_id)
                except Exception:
                    pass
                await _safe_send_photo_or_text(chat_id, photo, text_or_caption, kb)
        else:
            if msg.photo:
                try:
                    await bot.delete_message(msg.chat.id, msg.message_id)
                except Exception:
                    pass
                await bot.send_message(chat_id, text_or_caption, reply_markup=kb, parse_mode="HTML")
            else:
                try:
                    await msg.edit_text(text_or_caption, reply_markup=kb, parse_mode="HTML")
                except TelegramBadRequest as e:
                    if "message is not modified" not in str(e).lower():
                        await bot.send_message(chat_id, text_or_caption, reply_markup=kb, parse_mode="HTML")
    finally:
        await callback_query.answer()


async def send_today_menu(chat_id: int, user_id: int):
    today = now_msk_str()
    try:
        await bot.send_chat_action(chat_id, ChatAction.TYPING)
    except Exception:
        pass
    menu = await sheets_get_menu(today)
    await fsm.update_data(user_id, menu=menu, menu_idx=0)
    if not menu:
        await bot.send_message(chat_id, "Меню на сегодня ещё не добавлено. Попробуй позже.")
        return
    await show_menu_item(chat_id, user_id)


async def ensure_registered_and_show_menu(message: Message):
    user = message.from_user
    assert user is not None
    uid = user.id
    username = user.username or ""
    client = await sheets_find_client(uid)
    if client:
        await fsm.set_state(uid, "menu")
        await fsm.update_data(uid, username=username)
        await send_today_menu(message.chat.id, uid)
    else:
        await fsm.set_state(uid, "awaiting_name")
        await fsm.update_data(uid, username=username)
        await message.answer("Привет! Давай познакомимся. Напиши имя и фамилию 🙂")


async def _show_confirm(uid: int, msg: Message):
    data = await fsm.get_data(uid)
    tariff = data.get("chosen_tariff", "")
    address = data.get("chosen_address", "")
    timeslot = data.get("chosen_time", "")
    qty = int(data.get("qty", 1))
    pay_url = "https://pay.raschet.by/#00020132410010by.raschet01074440631101229286-1-6599530393354040.005802BY5913UNP_2918581506007Belarus6304D87F"
    kb = kb_confirm(payment_url=pay_url).as_markup()
    text = (
        "Проверь заказ:\n"
        f"• Тариф: <b>{h(tariff)}</b>\n"
        f"• Адрес: <b>{h(address)}</b>\n"
        f"• Время: <b>{h(timeslot)}</b>\n"
        f"• Количество: <b>{qty}</b>\n\n"
        "Подтвердить заказ?"
    )
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")


async def _finalize_order(call: CallbackQuery, payment_label: str):
    user = call.from_user
    if not user:
        return
    uid = user.id
    data = await fsm.get_data(uid)
    dish = data.get("chosen_dish")
    tariff = data.get("chosen_tariff", "")
    address = data.get("chosen_address")
    timeslot = data.get("chosen_time")
    qty = int(data.get("qty", 1))
    ok, err = await reserve_portions_for_today(dish, qty)
    if not ok:
        await call.answer(err or "Не удалось оформить: блюдо закончилось.", show_alert=True)
        await fsm.set_state(uid, "menu")
        await send_today_menu(call.message.chat.id, uid)
        return
    client = await sheets_find_client(uid)
    if not client:
        await release_portions_for_today(dish, qty)
        await call.answer("Не найден профиль клиента. Отправь /start для регистрации.", show_alert=True)
        await fsm.clear(uid)
        try:
            await call.message.edit_text("Ошибка профиля. Отправь /start для регистрации.", parse_mode="HTML")
        except TelegramBadRequest:
            try:
                await bot.delete_message(call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            await bot.send_message(call.message.chat.id, "Ошибка профиля. Отправь /start для регистрации.", parse_mode="HTML")
        return
    name = str(client.get("Имя", "")).strip()
    phone = str(client.get("Номер телефона", "")).strip()
    picked = format_selection_for_tariff(dish, tariff)
    try:
        await sheets_append_order(uid, name, phone, picked, tariff, address, timeslot, qty, payment_label)
    except Exception as e:
        await release_portions_for_today(dish, qty)
        logging.exception("Не удалось сохранить заказ в Sheets: %s", e)
        await call.answer("Не удалось сохранить заказ. Попробуй позже.", show_alert=True)
        return
    try:
        await crm_send_order(
            user_id=uid,
            name=name,
            phone=phone,
            address=address,
            timeslot=timeslot,
            dish=picked,
            qty=qty,
            payment_label=payment_label,
            tariff=tariff,
        )
    except Exception as e:
        logging.exception("crm_send_order unexpected error: %s", e)
    local = await fsm.get_data(uid)
    menu = local.get("menu", [])
    idx = local.get("menu_idx", 0)
    if 0 <= idx < len(menu):
        try:
            cur = int(str(menu[idx].get("Количество", "0")).strip() or "0")
        except ValueError:
            cur = 0
        menu[idx]["Количество"] = str(max(0, cur - 1))
        await fsm.update_data(uid, menu=menu)
    note = (
        f"🧾 Новый заказ ({payment_label}):\n"
        f"Имя: {h(name)}\nТелефон: {h(phone)}\nАдрес: {h(address)}\nТариф: {h(tariff)}\nКоличество: {qty}\nВремя доставки: {timeslot}\n"
    )
    await notify_recipients(note)
    text_ok = "Спасибо! Твой заказ принят ✅"
    kb = kb_show_menu_again().as_markup()
    try:
        await call.message.edit_text(text_ok, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, text_ok, reply_markup=kb, parse_mode="HTML")
    await fsm.set_state(uid, "menu")
    await fsm.update_data(uid, chosen_dish=None, chosen_address=None, chosen_time=None)
    await call.answer()


async def notify_recipients(text: str):
    if not NOTIFY_IDS:
        return
    for rid in NOTIFY_IDS:
        try:
            await bot.send_message(rid, text, parse_mode="HTML")
        except Exception:
            pass
        await asyncio.sleep(0.03)

