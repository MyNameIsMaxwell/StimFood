# -*- coding: utf-8 -*-
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

from app.config import ADDRESS_OPTIONS, TIME_SLOTS


def kb_send_contact():
    kb = ReplyKeyboardBuilder()
    kb.button(text="Поделиться контактом", request_contact=True)
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=True)


def kb_support():
    kb = ReplyKeyboardBuilder()
    kb.button(text="Связаться с нами")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=False)


def kb_menu_navigation(can_switch: bool, include_tariffs: bool = True, has_active_sub: bool = False) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()

    if can_switch:
        kb.button(text="◀️", callback_data="menu_prev")
    if can_switch:
        kb.button(text="▶️", callback_data="menu_next")
    if can_switch:
        kb.adjust(2)

    if include_tariffs:
        kb.row(
            InlineKeyboardButton(text="Максимум", callback_data="tariff:max"),
            InlineKeyboardButton(text="Стандарт+", callback_data="tariff:standard_plus"),
        )
        kb.row(
            InlineKeyboardButton(text="Стандарт (+суп)", callback_data="tariff:standard_soup"),
            InlineKeyboardButton(text="Стандарт (+салат)", callback_data="tariff:standard_salad"),
        )
        if has_active_sub:
            kb.row(InlineKeyboardButton(text="📋 Статус подписки", callback_data="sub:status"))
        else:
            kb.row(InlineKeyboardButton(text="🔥Умный заказ🔥", callback_data="sub:start"))

    kb.row(InlineKeyboardButton(text="Посмотреть всё меню", callback_data="menu_show_week"))
    return kb


def kb_choose_address() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for a in ADDRESS_OPTIONS:
        kb.button(text=a, callback_data=f"addr:{a}")
    kb.button(text="Ввести адрес", callback_data="addr_custom")
    kb.button(text="Назад", callback_data="back:menu")
    kb.adjust(2, 2, 1, 1)
    return kb


def kb_choose_time() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for t in TIME_SLOTS:
        kb.button(text=t, callback_data=f"time:{t}")
    kb.button(text="Назад", callback_data="back:addr")
    kb.adjust(2, 1)
    return kb


def kb_choose_qty() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="1", callback_data="qty:1"),
        InlineKeyboardButton(text="2", callback_data="qty:2"),
        InlineKeyboardButton(text="Больше", callback_data="qty:more")
    )
    kb.row(InlineKeyboardButton(text="5+1🎁", callback_data="qty:6"))
    kb.row(InlineKeyboardButton(text="Назад", callback_data="back:time"))
    return kb


def kb_confirm(payment_url: str | None = None) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()

    kb.row(InlineKeyboardButton(text="✅ Всё верно, оплатить при получении", callback_data="confirm_cash"))

    if payment_url:
        kb.row(InlineKeyboardButton(text="💳 Всё верно, оплатить онлайн (полное ФИО)", url=payment_url))
        kb.row(InlineKeyboardButton(text="✅ Я оплатил онлайн", callback_data="confirm_paid"))

    kb.row(InlineKeyboardButton(text="Назад", callback_data="back:time"))
    return kb


def kb_show_menu_again() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="Посмотреть меню на сегодня", callback_data="show_menu_again")
    kb.adjust(1)
    return kb


def kb_sub_choose_tariff() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="Максимум", callback_data="sub_tariff:max"),
        InlineKeyboardButton(text="Стандарт+", callback_data="sub_tariff:standard_plus"),
    )
    kb.row(
        InlineKeyboardButton(text="Стандарт (+суп)", callback_data="sub_tariff:standard_soup"),
        InlineKeyboardButton(text="Стандарт (+салат)", callback_data="sub_tariff:standard_salad"),
    )
    kb.row(InlineKeyboardButton(text="Назад", callback_data="sub_back:menu"))
    return kb


def kb_sub_choose_address() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for a in ADDRESS_OPTIONS:
        kb.button(text=a, callback_data=f"sub_addr:{a}")
    kb.button(text="Ввести адрес", callback_data="sub_addr_custom")
    kb.button(text="Назад", callback_data="sub_back:tariff")
    kb.adjust(2, 2, 1, 1)
    return kb


def kb_sub_choose_time() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for t in TIME_SLOTS:
        kb.button(text=t, callback_data=f"sub_time:{t}")
    kb.button(text="Назад", callback_data="sub_back:addr")
    kb.adjust(2, 1)
    return kb


def kb_sub_choose_days() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="5 дней", callback_data="sub_days:5")
    )
    kb.row(
        InlineKeyboardButton(text="10 дней (9+1 бесплатное)", callback_data="sub_days:10")
    )
    kb.row(
        InlineKeyboardButton(text="20 дней (18+2 бесплатных)", callback_data="sub_days:20")
    )

    kb.row(InlineKeyboardButton(text="Назад", callback_data="sub_back:time"))
    return kb


def kb_sub_confirm(payment_url: str | None = None) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✅ Оформить подписку, оплата при получении", callback_data="sub_confirm_cash"))
    if payment_url:
        kb.row(InlineKeyboardButton(text="💳 Оформить подписку, оплатить онлайн", url=payment_url))
        kb.row(InlineKeyboardButton(text="✅ Я оплатил онлайн", callback_data="sub_confirm_paid"))
    kb.row(InlineKeyboardButton(text="Назад", callback_data="sub_back:days"))
    return kb
