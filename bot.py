# -*- coding: utf-8 -*-
"""
Телеграм-бот: регистрация клиентов в Google Sheets + меню на сегодня с inline-переключением,
выбор адреса/времени и запись заказа в Google Sheets. Машина состояний хранится в SQLite.

Зависимости (установить):
    pip install aiogram==3.* gspread google-auth aiosqlite python-dotenv

Переменные окружения (создайте .env рядом со скриптом или задайте в системе):
    BOT_TOKEN=<токен телеграм-бота>
    GSHEET_ID=<ID Google Spreadsheet>
    GOOGLE_SERVICE_ACCOUNT_JSON=<путь до service_account.json>  (или GOOGLE_SERVICE_ACCOUNT_INFO=<JSON-строка>)

Структура таблиц:
    Лист "Клиенты": столбцы (в точности): telegram_id | Имя | username | Номер телефона | Дата регистрации
    Лист "Меню":    столбцы (в точности): День | Блюда | Фото | Количество
    Лист "Заказы":  столбцы (в точности): Дата | user_id | Имя | Номер телефона | Блюдо | Место доставки | Время доставки
"""

import asyncio
import json
import os
import re
import html
import logging
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatAction
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import (
    CallbackQuery,
    Message,
    ContentType,
    ReplyKeyboardRemove,
    InputMediaPhoto,
    InlineKeyboardButton,
    BufferedInputFile,
    BotCommand,
    MenuButtonCommands
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiohttp import ClientSession, ClientError
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# ---------- Константы настройки ----------
ADDRESS_OPTIONS = ["Цельсий", "Дубровская(СТиМ)", "Катин Бор(СТиМ)", "БОНШЕ"]
TIME_SLOTS = ["12-13", "13-14"]

DB_PATH = "fsm.sqlite3"

# ---------- Загрузка окружения ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
SERVICE_ACCOUNT_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SERVICE_ACCOUNT_INFO = os.getenv("GOOGLE_SERVICE_ACCOUNT_INFO")  # альтернатива пути
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0") or 0)

CRM_ENABLED = os.getenv("CRM_ENABLED")
CRM_ENDPOINT = os.getenv("CRM_ENDPOINT")
CRM_IDENTIFIER = os.getenv("CRM_IDENTIFIER")
CRM_WEBAPI_KEY = os.getenv("CRM_WEBAPI_KEY")

# список дополнительных получателей уведомлений
_raw = os.getenv("NOTIFY_IDS", "") or ""
NOTIFY_IDS: List[int] = []
for part in _raw.split(","):
    p = part.strip()
    if p.isdigit():
        NOTIFY_IDS.append(int(p))

if not BOT_TOKEN or not GSHEET_ID or not (SERVICE_ACCOUNT_JSON_PATH or SERVICE_ACCOUNT_INFO):
    raise RuntimeError(
        "Отсутствуют переменные окружения BOT_TOKEN, GSHEET_ID "
        "и GOOGLE_SERVICE_ACCOUNT_JSON (или GOOGLE_SERVICE_ACCOUNT_INFO)."
    )


# ---------- Утилиты ----------

def normalize_phone(text: str) -> Optional[str]:
    """Простая валидация телефона: оставляем + и цифры, проверяем длину >= 10."""
    digits = re.sub(r"[^\d+]", "", text).strip()
    if digits.startswith("8"):  # частый случай РФ
        digits = "+7" + digits[1:]
    if digits.startswith("+") and len(re.sub(r"\D", "", digits)) >= 10:
        return digits
    if len(re.sub(r"\D", "", digits)) >= 10:
        return "+" + re.sub(r"\D", "", digits)
    return None


WEEKDAYS_RU = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]


def parse_ddmmyyyy(s: str) -> Optional[datetime]:
    s = extract_ddmmyyyy(s)
    try:
        return datetime.strptime(s, "%d.%m.%Y")
    except Exception:
        return None


def weekday_ru_from_ddmmyyyy(s: str) -> str:
    dt = parse_ddmmyyyy(s)
    if not dt:
        return ""
    # isoweekday: Mon=1..Sun=7 -> наш индекс 0..6
    return WEEKDAYS_RU[dt.isoweekday() - 1]


def extract_ddmmyyyy(s: str) -> str:
    """
    Возвращает дату в формате dd.mm.yyyy из произвольной строки s.
    Поддерживает:
      - 'YYYY-MM-DD HH:MM:SS'  -> 'dd.mm.yyyy'
      - 'YYYY-MM-DD'           -> 'dd.mm.yyyy'
      - 'dd.mm.yyyy' (как есть)
    Если ничего не нашли — возвращает сегодняшнюю дату в 'dd.mm.yyyy'.
    """
    if not s:
        return datetime.now().strftime("%d.%m.%Y")

    s = s.strip()

    # 1) dd.mm.yyyy
    m = re.search(r'(\b\d{2}\.\d{2}\.\d{4}\b)', s)
    if m:
        return m.group(1)

    # 2) yyyy-mm-dd (с опциональным временем)
    m = re.search(r'\b(\d{4})-(\d{2})-(\d{2})\b', s)
    if m:
        y, mo, d = m.groups()
        return f"{d}.{mo}.{y}"

    # фоллбэк: сегодня
    return datetime.now().strftime("%d.%m.%Y")

def now_msk_str() -> str:
    return (datetime.now() + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def h(s: str) -> str:
    return html.escape(s or "", quote=False)

def msk_now_dt() -> datetime:
    # та же базовая идея, что и now_msk_str(), только возвращаем datetime
    return datetime.now() + timedelta(hours=3)

def start_of_week_msk_dt() -> datetime:
    dt = msk_now_dt()
    # ISO: Monday=0 … Sunday=6
    monday = dt - timedelta(days=dt.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0)

def start_of_week_msk_str() -> str:
    return start_of_week_msk_dt().strftime("%Y-%m-%d %H:%M:%S")

def seconds_until_next_930_msk() -> float:
    now = msk_now_dt()
    target = now.replace(hour=9, minute=31, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


def format_menu_for_broadcast(items: List[Dict[str, Any]]) -> str:
    # выводим список блюд на сегодня построчно
    lines = []
    for it in items:
        dish = str(it.get("Блюда", "")).strip()
        if dish:
            lines.append(f"{dish}")
    return "\n".join(lines)

# ---------- Google Sheets клиент (синхронный gspread вызываем через asyncio.to_thread) ----------

class GoogleSheetsClient:
    def __init__(self, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self._gc = self._build_client()
        self._sh = self._gc.open_by_key(self.spreadsheet_id)

    def _build_client(self):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        if SERVICE_ACCOUNT_INFO:
            info = json.loads(SERVICE_ACCOUNT_INFO)
            creds = Credentials.from_service_account_info(info, scopes=scopes)
        else:
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON_PATH, scopes=scopes)
        return gspread.authorize(creds)

    # ---- Листы ----
    def ws_clients(self):
        return self._sh.worksheet("Клиенты")

    def ws_menu(self):
        return self._sh.worksheet("Меню")

    def ws_orders(self):
        return self._sh.worksheet("Заказы")

    # ---- Операции с клиентами ----
    def find_client(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        ws = self.ws_clients()
        records = ws.get_all_records()
        for row in records:
            # В таблице telegram_id может быть числом или строкой
            try:
                if str(row.get("telegram_id", "")).strip() == str(telegram_id):
                    return row
            except Exception:
                continue
        return None

    def add_client(self, telegram_id: int, name: str, username: str, phone: str):
        ws = self.ws_clients()
        ws.append_row(
            [
                str(telegram_id),
                name,
                username or "",
                phone,
                now_msk_str(),
            ],
            value_input_option="USER_ENTERED",
        )

    # ---- Операции с меню ----
    def get_menu_for_day(self, day_name: str) -> List[Dict[str, Any]]:
        ws = self.ws_menu()
        all_values = ws.get_all_values()
        if not all_values:
            return []
        headers = all_values[0]
        try:
            day_col = headers.index("День")
        except ValueError:
            return []

        want_day = extract_ddmmyyyy(day_name)
        items: List[Dict[str, Any]] = []
        for i in range(1, len(all_values)):
            row = all_values[i]
            # аккуратно соберём запись
            rec = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
            cell_day = extract_ddmmyyyy(str(rec.get("День", "")))
            if cell_day == want_day:
                items.append(rec)
        return items

    # ---- Операции с заказами ----
    def append_order(
            self,
            date_str: str,
            user_id: int,
            name: str,
            phone: str,
            dish: str,
            tariff: str,
            address: str,
            timeslot: str,
            qty: int,
            payment_label: str
    ):
        ws = self.ws_orders()
        ws.spreadsheet.values_append(
            f"{ws.title}!A1:J1",
            params={
                "valueInputOption": "USER_ENTERED",
                "insertDataOption": "INSERT_ROWS",
            },
            body={"values": [[date_str, str(user_id), name, phone, dish, tariff, address, timeslot, int(qty), payment_label]]},
        )


    def find_menu_row_by_day_and_dish(self, day_name: str, dish_name: str) -> tuple[int | None, dict | None]:
        ws = self.ws_menu()
        all_values = ws.get_all_values()
        if not all_values:
            return None, None

        headers = all_values[0]
        try:
            day_col = headers.index("День")
            dish_col = headers.index("Блюда")
        except ValueError:
            return None, None

        want_day = extract_ddmmyyyy(day_name)
        want_dish = str(dish_name or "").strip().lower()

        # Берём по regex дату из ячейки 'День' вида dd.mm.yyyy (или приводим)
        for i in range(1, len(all_values)):
            row = all_values[i]
            if len(row) <= max(day_col, dish_col):
                continue

            cell_day_raw = str(row[day_col]).strip()
            cell_dish = str(row[dish_col]).strip().lower()

            # из ячейки 'День' вытащим dd.mm.yyyy, если там что-то ещё
            cell_day = extract_ddmmyyyy(cell_day_raw)

            if cell_day == want_day and cell_dish == want_dish:
                record = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
                return i + 1, record  # 1-based индекс строки
        return None, None

        # # проходим по строкам со второй (index=1) — это 2-я строка в таблице
        # for i in range(1, len(all_values)):
        #     row = all_values[i]
        #     if len(row) <= max(day_col, dish_col):
        #         continue
        #     if str(row[day_col]).strip() == str(day_name).strip() and str(row[dish_col]).strip() == str(
        #             dish_name).strip():
        #         # собираем record по заголовкам (безопасно)
        #         record = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
        #         # индекс строки для gspread 1-based → i+1
        #         return i + 1, record
        # return None, None

    def get_quantity_by_row(self, row_index: int) -> int:
        ws = self.ws_menu()
        headers = ws.row_values(1)
        qty_col = headers.index("Количество") + 1  # 1-based
        val = ws.cell(row_index, qty_col).value
        try:
            return int(str(val).strip())
        except Exception:
            return 0

    def set_quantity_by_row(self, row_index: int, new_qty: int):
        ws = self.ws_menu()
        headers = ws.row_values(1)
        qty_col = headers.index("Количество") + 1  # 1-based
        ws.update_cell(row_index, qty_col, new_qty)

    def get_week_menu(self, start_day_str: str, days: int = 7) -> List[Dict[str, Any]]:
        ws = self.ws_menu()
        all_values = ws.get_all_values()
        if not all_values:
            return []
        headers = all_values[0]
        if "День" not in headers or "Блюда" not in headers:
            return []

        day_idx = headers.index("День")
        dish_idx = headers.index("Блюда")

        start_day = parse_ddmmyyyy(start_day_str) or datetime.now()
        end_day = (start_day + timedelta(days=days - 1)).date()

        items = []
        for i in range(1, len(all_values)):
            row = all_values[i]
            if len(row) <= max(day_idx, dish_idx):
                continue
            day_cell = extract_ddmmyyyy(str(row[day_idx]))
            dt = parse_ddmmyyyy(day_cell)
            if not dt:
                continue
            if start_day.date() <= dt.date() <= end_day:
                rec = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
                items.append(rec)
        # отсортируем по дате
        items.sort(key=lambda r: parse_ddmmyyyy(str(r.get("День", ""))) or datetime.now())
        return items

    def ws_overorders(self):
        # создай лист в таблице с названием ровно "Заказы свыше"
        return self._sh.worksheet("Заказы свыше")

    def append_overorder(self, date_str: str, user_id: int, name: str, phone: str, dish: str, address: str,
                         timeslot: str):
        ws = self.ws_overorders()
        ws.append_row(
            [date_str, str(user_id), name, phone, dish, address, timeslot],
            value_input_option="USER_ENTERED",
        )

    def get_all_client_chat_ids(self) -> List[int]:
        ws = self.ws_clients()
        ids = []
        for row in ws.get_all_records():
            tid = str(row.get("telegram_id", "")).strip()
            if tid.isdigit():
                try:
                    ids.append(int(tid))
                except Exception:
                    pass
        return ids


# Асинхронные обертки для gspread
_sheets_client: Optional[GoogleSheetsClient] = None


def get_sheets_client() -> GoogleSheetsClient:
    global _sheets_client
    if _sheets_client is None:
        _sheets_client = GoogleSheetsClient(GSHEET_ID)
    return _sheets_client


async def sheets_find_client(telegram_id: int) -> Optional[Dict[str, Any]]:
    return await asyncio.to_thread(get_sheets_client().find_client, telegram_id)


async def sheets_add_client(telegram_id: int, name: str, username: str, phone: str):
    await asyncio.to_thread(get_sheets_client().add_client, telegram_id, name, username, phone)


async def sheets_get_menu(day_name: str) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(get_sheets_client().get_menu_for_day, day_name)


async def sheets_find_menu_row(day_name: str, dish_name: str):
    return await asyncio.to_thread(get_sheets_client().find_menu_row_by_day_and_dish, day_name, dish_name)


async def sheets_get_quantity_by_row(row_index: int) -> int:
    return await asyncio.to_thread(get_sheets_client().get_quantity_by_row, row_index)


async def sheets_set_quantity_by_row(row_index: int, new_qty: int):
    return await asyncio.to_thread(get_sheets_client().set_quantity_by_row, row_index, new_qty)


async def sheets_get_week_menu(start_day_str: str, days: int = 7) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(get_sheets_client().get_week_menu, start_day_str, days)

async def sheets_get_all_client_ids() -> List[int]:
    return await asyncio.to_thread(get_sheets_client().get_all_client_chat_ids)


async def sheets_append_overorder(user_id: int, name: str, phone: str, dish: str):
    date_str = now_msk_str()
    # адрес/время нам неизвестны на этом шаге — пишем пусто
    await asyncio.to_thread(
        get_sheets_client().append_overorder,
        date_str, user_id, name, phone, dish, "", ""
    )


async def crm_send_order(
    name: str,
    phone: str,
    address: str,
    timeslot: str,
    dish: str,
    qty: int,
    payment_label: str,
    tariff: str = "",
) -> bool:
    """
    Отправляет заказ в CRM. Возвращает True/False (успех/неуспех).
    Не бросает исключения наружу — логирует и молча возвращает False.
    """
    if not CRM_ENABLED:
        return False
    if not (CRM_ENDPOINT and CRM_IDENTIFIER and CRM_WEBAPI_KEY):
        logging.warning("CRM is enabled, but credentials are not set")
        return False

    comment = "\n".join([
        str(tariff or ""),
        str(qty or ""),
        str(timeslot or ""),
        str(payment_label or ""),
    ]).strip()

    # Формируем x-www-form-urlencoded
    payload = {
        "identifier": CRM_IDENTIFIER,
        "webApiKey": CRM_WEBAPI_KEY,
        "name": name,
        "phone": phone,
        "address_text": address,
        "is_retail_order": 1,
        "comment": comment,
        # "additional[Кол-во]": qty,
        # "additional[Время доставки]": timeslot,
        # "additional[Оплата]": payment_label,
    }

    try:
        async with ClientSession() as session:
            async with session.post(CRM_ENDPOINT, data=payload, timeout=20) as resp:
                # CRM обычно отвечает 200/2xx на успех
                ok = 200 <= resp.status < 300
                if not ok:
                    body = await resp.text()
                    logging.error("CRM responded %s: %s", resp.status, body[:500])
                return ok
    except ClientError as e:
        logging.exception("CRM request failed: %s", e)
        return False
    except Exception as e:
        logging.exception("Unexpected CRM error: %s", e)
        return False


async def reserve_portions_for_today(dish_name: str, qty: int) -> tuple[bool, str | None]:
    """
    Уменьшает Количество на qty для сегодняшнего дня и указанного блюда.
    Вернёт (True, None) если успешно, иначе (False, "причина").
    """
    day = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_index, _ = await sheets_find_menu_row(day, dish_name)
    if not row_index:
        return False, "Позиция меню не найдена."

    current = await sheets_get_quantity_by_row(row_index)
    try:
        need = int(qty)
    except Exception:
        need = 1
    if need <= 0:
        need = 1

    if current < need:
        return False, f"Доступно только {current} шт."

    await sheets_set_quantity_by_row(row_index, current - need)
    return True, None


async def release_portions_for_today(dish_name: str, qty: int):
    """Возвращает qty порций обратно (на случай ошибки при записи заказа)."""
    day = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_index, _ = await sheets_find_menu_row(day, dish_name)
    if not row_index:
        return
    current = await sheets_get_quantity_by_row(row_index)
    try:
        add_back = int(qty)
    except Exception:
        add_back = 1
    if add_back <= 0:
        add_back = 1
    await sheets_set_quantity_by_row(row_index, current + add_back)

logger = logging.getLogger("orders")

async def sheets_append_order(
        user_id: int,
        name: str,
        phone: str,
        dish: str,
        tariff: str,
        address: str,
        timeslot: str,
        qty: int,
        payment_label: str,

):
    date_str = now_msk_str()
    await asyncio.to_thread(
        get_sheets_client().append_order,
        date_str, user_id, name, phone, dish, tariff, address, timeslot, qty, payment_label
    )

async def notify_recipients(text: str):
    if not NOTIFY_IDS:
        return
    for rid in NOTIFY_IDS:
        try:
            await bot.send_message(rid, text, parse_mode="HTML")
        except Exception:
            pass
        await asyncio.sleep(0.03)


# ---------- SQLite FSM ----------

class FSMStorage:
    """Простейшее хранилище состояний в SQLite: state (TEXT) + data (JSON)."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS fsm_states (
                    user_id INTEGER PRIMARY KEY,
                    state TEXT,
                    data TEXT
                )
            """)
            await db.commit()

    async def get_state(self, user_id: int) -> Optional[str]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT state FROM fsm_states WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else None

    async def set_state(self, user_id: int, state: Optional[str]):
        async with aiosqlite.connect(self.db_path) as db:
            if state is None:
                await db.execute("DELETE FROM fsm_states WHERE user_id = ?", (user_id,))
            else:
                await db.execute("""
                    INSERT INTO fsm_states (user_id, state, data)
                    VALUES (?, ?, COALESCE((SELECT data FROM fsm_states WHERE user_id = ?), '{}'))
                    ON CONFLICT(user_id) DO UPDATE SET state=excluded.state
                """, (user_id, state, user_id))
            await db.commit()

    async def get_data(self, user_id: int) -> Dict[str, Any]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT data FROM fsm_states WHERE user_id = ?", (user_id,)) as cur:
                row = await cur.fetchone()
                if row and row[0]:
                    try:
                        return json.loads(row[0])
                    except Exception:
                        return {}
                return {}

    async def update_data(self, user_id: int, **kwargs):
        data = await self.get_data(user_id)
        data.update(kwargs)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO fsm_states (user_id, state, data)
                VALUES (?, NULL, ?)
                ON CONFLICT(user_id) DO UPDATE SET data=excluded.data
            """, (user_id, json.dumps(data, ensure_ascii=False)))
            await db.commit()

    async def clear(self, user_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM fsm_states WHERE user_id = ?", (user_id,))
            await db.commit()


fsm = FSMStorage(DB_PATH)

# ---------- Телеграм-бот ----------

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
admin_router = Router(name="admin")
router = Router()
dp.include_router(router)


# ---- Клавиатуры ----

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


def kb_menu_navigation(can_switch: bool, include_tariffs: bool = True) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()

    # Стрелки
    if can_switch:
        kb.button(text="◀️", callback_data="menu_prev")
    if can_switch:
        kb.button(text="▶️", callback_data="menu_next")
    if can_switch:
        kb.adjust(2)

    # Ряд с тарифами
    if include_tariffs:
        kb.row(
            InlineKeyboardButton(text="Максимум", callback_data="tariff:max"),
            InlineKeyboardButton(text="Стандарт+",  callback_data="tariff:standard_plus"),
        )
        kb.row(
            InlineKeyboardButton(text="Стандарт (+суп)", callback_data="tariff:standard_soup"),
            InlineKeyboardButton(text="Стандарт (+салат)", callback_data="tariff:standard_salad"),
        )

    # Внизу — "Посмотреть всё меню"
    kb.row(InlineKeyboardButton(text="Посмотреть всё меню", callback_data="menu_show_week"))
    return kb



def kb_choose_address() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for a in ADDRESS_OPTIONS:
        kb.button(text=a, callback_data=f"addr:{a}")
    kb.button(text="Ввести адрес", callback_data="addr_custom")
    kb.button(text="Назад", callback_data="back:menu")
    kb.adjust(2, 2, 1, 1)  # 2-2 адреса, потом "Ввести адрес", потом "Назад"
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


def normalize_photo_url(url: str) -> str:
    if not url:
        return url
    url = url.strip()

    # file/d/<ID>/view
    m = re.search(r"drive\.google\.com/(?:file/d/|uc\?id=)([^/&#?]+)", url)
    if m:
        file_id = m.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"

    return url

def _strip_header_line(text: str) -> list[str]:
    """Убираем 'Блюдо дня:' и пустые строки, сохраняем эмодзи и регистр."""
    lines = [ln.strip() for ln in text.splitlines()]
    lines = [ln for ln in lines if ln]  # non-empty
    if lines and re.match(r"^Блюдо дня[:：]?\s*$", lines[0], flags=re.IGNORECASE):
        lines = lines[1:]
    return lines

def split_menu_components(dish_text: str) -> dict:
    """
     КЛАССИФИКАЦИЯ ПО ПОЗИЦИИ (а не по словам).
     Ожидаемый порядок: [0]=суп, [1]=горячее, [2]=салат, [3]=напиток.
     Если строк меньше/больше — берём доступные и лишние уводим в 'other'.
     """
    lines = _strip_header_line(dish_text)

    soup = lines[0] if len(lines) > 0 else None
    hot = lines[1] if len(lines) > 1 else None
    salad = lines[2] if len(lines) > 2 else None
    drink = lines[3] if len(lines) > 3 else None
    other = lines[4:] if len(lines) > 4 else []

    return {"soup": soup, "hot": hot, "salad": salad, "drink": drink, "other": other}

def format_selection_for_tariff(dish_text: str, tariff: str) -> str:
    """
    Собирает строки под выбранный тариф.
    Порядок — суп → горячее → салат → напиток (как ты просил).
    """
    p = split_menu_components(dish_text)

    # что включаем по тарифам
    want = []
    t = tariff.lower()
    if "максимум" in t:
        want = ["soup", "hot", "salad", "drink"]
    elif "стандарт+" in t:
        want = ["soup", "hot", "salad"]
    elif "суп" in t:
        want = ["soup", "hot"]
    elif "салат" in t:
        want = ["hot", "salad"]
    else:
        # дефолт: только горячее
        want = ["hot"]

    ordered = []
    order = ["soup", "hot", "salad", "drink"]
    # соблюдаем глобальный порядок, но берём только нужные
    for key in order:
        if key in want and p.get(key):
            ordered.append(p[key])

    # если вдруг ничего не распознали — покажем весь текст без заголовка
    if not ordered:
        ordered = _strip_header_line(dish_text)

    return "\n".join(ordered)


# ---- Сервисные шаги ----

async def ensure_registered_and_show_menu(message: Message):
    """Проверка регистрации. Если есть — показываем меню. Если нет — спрашиваем имя."""
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
    # Сохраним меню целиком и текущий индекс в FSM
    await show_menu_item(chat_id, user_id)


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
    """
    1) Пытается отправить фото по URL (после normalize_photo_url)
    2) Если Telegram ругнулся (web page content и пр.) — скачивает и отправляет как байты
    3) Если не вышло — отправляет текст с пометкой
    """
    url = normalize_photo_url(photo_url) if photo_url else ""

    # 1) Прямой URL
    if url:
        try:
            await bot.send_photo(chat_id, photo=url, caption=caption, reply_markup=reply_markup)
            return
        except TelegramBadRequest as e:
            # например: wrong type of the web page content
            if "wrong type of the web page content" not in str(e).lower():
                # другие ошибки тоже попробуем обойти скачиванием
                pass
        except Exception:
            pass

    # 2) Скачиваем и re-upload
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

    # 3) Фоллбэк — текст
    await bot.send_message(
        chat_id,
        f"{caption}\n(Фото недоступно по ссылке)",
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )


async def edit_text_or_caption(msg, text: str, reply_markup=None, parse_mode: str | None = "HTML"):
    """
    Если msg с фото — редактируем caption, иначе text.
    Возвращает True, если редактирование прошло; False, если пришлось удалять.
    """
    try:
        if getattr(msg, "photo", None):
            await msg.edit_caption(caption=text, reply_markup=reply_markup, parse_mode=parse_mode)
        else:
            await msg.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        return True
    except TelegramBadRequest:
        # Нельзя редактировать текущий тип (или другое ограничение) — удалим и пришлём новое
        try:
            await msg.bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass
        await msg.bot.send_message(msg.chat.id, text, reply_markup=reply_markup, parse_mode=parse_mode)
        return False


async def _edit_media_smart(msg, photo_url: str, caption: str, kb) -> bool:
    """
    Пытается отредактировать СУЩЕСТВУЮЩЕЕ сообщение-карточку с фото:
    - сперва через URL (после normalize_photo_url),
    - если не получилось — скачивает и редактирует байтами.
    Возвращает True, если удалось отредактировать медиа; False — если нет.
    """
    url = normalize_photo_url(photo_url) if photo_url else ""
    if not url:
        return False

    # 1) Попытка как URL
    try:
        await msg.edit_media(
            InputMediaPhoto(media=url, caption=caption),
            reply_markup=kb
        )
        return True
    except TelegramBadRequest as e:
        if "wrong type of the web page content" not in str(e).lower():
            # другая ошибка — попробуем скачать
            pass
    except Exception:
        pass

    # 2) Скачиваем и редактируем байтами
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
    """
    Если msg с фото/медиа — удалить и отправить новое текстовое.
    Если текст — отредактировать.
    """
    try:
        if getattr(msg, "photo", None) or getattr(msg, "video", None) or getattr(msg, "document", None):
            # Редактировать text у медиа нельзя → удаляем и шлём новое
            try:
                await msg.bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
            await msg.bot.send_message(msg.chat.id, text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await msg.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest:
        # Fallback
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
    """
    Карточка «меню на сегодня».
    - Если у блюда есть фото — показываем фото + caption.
    - Если фото нет — обычный текст.
    - Если приходим из callback и текущее сообщение текстовое, а у блюда есть фото —
      удаляем текст и отправляем фото (чтобы снова была карточка с изображением).
    """
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

    # Первый показ (после /start или после успешной регистрации)
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

    # Пришли из callback — нужно «привести» текущее сообщение к нужному виду
    msg = callback_query.message

    try:
        if photo:
            if msg.photo:
                # Было фото → меняем медиа/подпись
                ok = await _edit_media_smart(msg, photo, text_or_caption, kb)
                if not ok:
                    try:
                        await bot.delete_message(msg.chat.id, msg.message_id)
                    except Exception:
                        pass
                    await _safe_send_photo_or_text(chat_id, photo, text_or_caption, kb)
            else:
                # Был текст (например, после недельного меню) → удаляем и шлём фото
                try:
                    await bot.delete_message(msg.chat.id, msg.message_id)
                except Exception:
                    pass
                await _safe_send_photo_or_text(chat_id, photo, text_or_caption, kb)
        else:
            # Фото нет → должна быть текстовая карточка
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


async def _show_confirm(uid: int, msg: Message):
    data = await fsm.get_data(uid)
    # dish = data.get("chosen_dish", "")
    tariff = data.get("chosen_tariff", "")
    address = data.get("chosen_address", "")
    timeslot = data.get("chosen_time", "")
    qty = int(data.get("qty", 1))

    pay_url = "https://pay.raschet.by/#00020132410010by.raschet01074440631101229286-1-2181530393354040.005802BY5913UNP_2918581506007Belarus63044DC0"
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
        # если редактировать нельзя — присылаем новое
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

    # резерв порции
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

    # запись в Заказы (с логом)
    try:
        await sheets_append_order(uid, name, phone, picked, tariff, address, timeslot, qty, payment_label,)
    except Exception as e:
        # откат резерва, алерт и лог
        await release_portions_for_today(dish, qty)
        logging.exception("Не удалось сохранить заказ в Sheets: %s", e)
        await call.answer("Не удалось сохранить заказ. Попробуй позже.", show_alert=True)
        return

    try:
        await crm_send_order(
            name=name,
            phone=phone,
            address=address,
            timeslot=timeslot,
            dish=dish,
            qty=qty,
            payment_label=payment_label,
            tariff=tariff,
        )
    except Exception as e:
        logging.exception("crm_send_order unexpected error: %s", e)

    # обновляем локальный кеш количества
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

    # уведомление получателям
    note = (
        f"🧾 Новый заказ ({payment_label}):\n"
        f"Имя: {h(name)}\nТелефон: {h(phone)}\nАдрес: {h(address)}\nТариф: {h(tariff)}\nКоличество: {qty}\nВремя доставки: {timeslot}\n"
    )
    await notify_recipients(note)

    # ответ пользователю
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
# ---------- Хэндлеры ----------

@router.message(Command("menu"))
async def cmd_menu(message: Message):
    user = message.from_user
    if not user:
        return
    uid = user.id
    # Переведём FSM в состояние "menu" и покажем меню
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
ЕРИП - Сервис E-POS - Ввести номер лицевого счета ( 29286-1-2181 ) - ввести ФИО и сумму
Приятного аппетита 😋""")


@router.message(Command("send_all"))
async def admin_broadcast(message: Message):
    if not ADMIN_CHAT_ID or message.from_user.id != ADMIN_CHAT_ID:
        return await message.answer("Нет прав для рассылки.")

    # текст после команды
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
        await asyncio.sleep(0.05)  # мягко, чтобы не упереться в лимиты

    await message.answer(f"Рассылка завершена.\nУспешно: {sent}\nОшибок: {fail}")


@router.message(CommandStart())
async def cmd_start(message: Message):
    await ensure_registered_and_show_menu(message)


@router.message(F.content_type == ContentType.TEXT)
async def text_handler(message: Message):
    user = message.from_user
    if not user:
        return
    uid = user.id
    state = await fsm.get_state(uid)

    # пользователь пишет в поддержку
    if state == "awaiting_support_message":
        if ADMIN_CHAT_ID:
            kb = InlineKeyboardBuilder()
            kb.button(text="Ответить", callback_data=f"support_reply:{uid}")

            admin_text = (
                f"📬 <b>Новое сообщение от пользователя</b>\n"
                f"ID: <code>{uid}</code>\n"
                f"Username: @{user.username or '-'}\n"
                f"Имя: {h(user.full_name or '-')}\n\n"
                f"{h(message.text)}"
            )
            try:
                await bot.send_message(ADMIN_CHAT_ID, admin_text, reply_markup=kb.as_markup(), parse_mode="HTML")
            except Exception:
                # если админ не нажал Start боту — сюда попадём
                pass
        await message.answer("Спасибо! Сообщение передано оператору.")
        await fsm.set_state(uid, "menu")
        return

    # админ отвечает
    if ADMIN_CHAT_ID and uid == ADMIN_CHAT_ID:
        admin_state = await fsm.get_state(ADMIN_CHAT_ID)
        if admin_state == "awaiting_support_reply":
            data_admin = await fsm.get_data(ADMIN_CHAT_ID)
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
                    await message.answer(
                        "Не удалось отправить ответ пользователю. Возможно, он ещё не начинал диалог с ботом.")
            await fsm.set_state(ADMIN_CHAT_ID, None)
            return

    # Ожидаем имя при регистрации
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

    # Ожидаем телефон
    if state == "awaiting_phone":
        # Пользователь мог прислать текст вместо контакта
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
        await fsm.update_data(uid, phone=phone)  # сохраним локально
        # Уберем клавиатуру
        await message.answer("Спасибо! Регистрация завершена ✅", reply_markup=ReplyKeyboardRemove())
        await send_today_menu(message.chat.id, uid)
        return

    if state == "awaiting_custom_address":
        addr = message.text.strip()
        if len(addr) < 5:
            await message.answer("Слишком короткий адрес. Введи адрес подробнее, пожалуйста.")
            return
        await fsm.update_data(uid, chosen_address=addr)
        await fsm.set_state(uid, "choose_time")
        kb = kb_choose_time().as_markup()
        await message.answer(f"Адрес доставки: <b>{h(addr)}</b>\n\nТеперь выбери время доставки:", reply_markup=kb,
                             parse_mode="HTML")
        return

    if state == "awaiting_qty_manual":
        raw = (message.text or "").strip()
        if not re.fullmatch(r"\d{1,3}", raw) or int(raw) < 1:
            await message.answer("Нужно число ≥ 1. Попробуй ещё раз.")
            return
        qty = int(raw)
        await fsm.update_data(uid, qty=qty)
        # можно вернуть state к choose_qty, но сразу показываем подтверждение:
        await _show_confirm(uid, message)
        return

    # По умолчанию — если уже зарегистрирован, но пользователь пишет текст
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


# ---- CallbackQuery: меню навигация и выбор ----

@router.callback_query(F.data == "menu_prev")
async def cb_menu_prev(call: CallbackQuery):
    uid = call.from_user.id
    data = await fsm.get_data(uid)
    menu = data.get("menu", [])
    if not menu:
        return await call.answer("Меню пустое.")
    if len(menu) == 1:
        return await call.answer("Сегодня только одно блюдо 🙂")  # стрелок нет, но если как-то нажали

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


# @router.callback_query(F.data == "menu_choose")
# async def cb_menu_choose(call: CallbackQuery):
#     uid = call.from_user.id
#     data = await fsm.get_data(uid)
#     menu = data.get("menu", [])
#     if not menu:
#         return await call.answer("Меню пустое.")
#
#     idx = data.get("menu_idx", 0)
#     dish = str(menu[idx].get("Блюда", "")).strip()
#
#     # Проверка остатков (без списания)
#     day = now_msk_str()
#     row_index, _ = await sheets_find_menu_row(day, dish)
#     if not row_index:
#         return await call.answer("Позиция меню не найдена.", show_alert=True)
#     qty = await sheets_get_quantity_by_row(row_index)
#     if qty <= 0:
#         client = await sheets_find_client(uid)
#         if client:
#             name = str(client.get("Имя", "")).strip()
#             phone = str(client.get("Номер телефона", "")).strip()
#             await sheets_append_overorder(uid, name, phone, dish)
#         return await call.answer("Увы, это блюдо уже закончилось на сегодня",
#                                  show_alert=True)
#
#     # Сохраняем выбор
#     await fsm.update_data(uid, chosen_dish=dish)
#     await fsm.set_state(uid, "choose_address")
#
#     # Клавиатура и ТЕКСТ
#     kb = kb_choose_address().as_markup()
#     text = f"Вы выбрали: \n<b>{h(dish)}</b>\n\nВыбери адрес доставки:"
#
#     # ❗️Всегда удаляем карточку (фото/текст) и шлём НОВОЕ текстовое сообщение
#     try:
#         await bot.delete_message(call.message.chat.id, call.message.message_id)
#     except Exception:
#         pass
#     await bot.send_message(call.message.chat.id, text, reply_markup=kb, parse_mode="HTML")
#     await call.answer()

@router.callback_query(F.data.startswith("tariff:"))
async def cb_choose_tariff(call: CallbackQuery):
    uid = call.from_user.id

    data = await fsm.get_data(uid)
    menu = data.get("menu", [])
    if not menu:
        return await call.answer("Меню пустое.")

    idx = data.get("menu_idx", 0)
    dish = str(menu[idx].get("Блюда", "")).strip()

    # Проверка остатков (без списания)
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

    # Дешифруем «код» тарифа в человекочитаемое
    code = call.data.split("tariff:", 1)[1]
    mapping = {"max": "Максимум", "standard_plus": "Стандарт+", "standard_soup": "Стандарт (+суп)", "standard_salad": "Стандарт (+салат)"}
    tariff = mapping.get(code, code)

    # Сохраняем в FSM
    await fsm.update_data(uid, chosen_dish=dish, chosen_tariff=tariff)
    await fsm.set_state(uid, "choose_address")

    # Формируем подборку строк под выбранный тариф
    picked = format_selection_for_tariff(dish, tariff)

    # Переходим к выбору адреса (всегда текстом, без фото)
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

    # ТОЛЬКО edit_text (если что, шлём новое)
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
    await _finalize_order(call, payment_label="оплата при получении")

@router.callback_query(F.data == "confirm_paid")
async def cb_confirm_paid(call: CallbackQuery):
    await _finalize_order(call, payment_label="оплачено онлайн")

# ---- Назад ----

@router.callback_query(F.data.startswith("back:"))
async def cb_back(call: CallbackQuery):
    uid = call.from_user.id
    target = call.data.split("back:", 1)[1]

    if target == "menu" or target == "root":
        await fsm.set_state(uid, "menu")
        # show_menu_item сам разрулит: если текущая карточка текст → удалит и пришлёт фото
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
            await bot.send_message(call.message.chat.id, text, reply_markup=kb_choose_qty().as_markup(),
                                   parse_mode="HTML")
        await call.answer()
        return

    # fallback
    await fsm.set_state(uid, "menu")
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "show_menu_again")
async def cb_show_menu_again(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "menu")
    # Обновим меню из Google Sheets, чтобы карточка была актуальной
    today = now_msk_str()
    fresh_menu = await sheets_get_menu(today)
    await fsm.update_data(uid, menu=fresh_menu, menu_idx=0)
    # отрисуем карточку в том же сообщении
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "menu_show_week")
async def cb_menu_show_week(call: CallbackQuery):
    # # берём «сегодня» как старт
    # start = now_msk_str()
    # старт — понедельник текущей недели, 00:00 МСК
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
        # lines.append(f"<b>{day} ({wd}):</b> \n {h(dish)}")
        lines.append(f"<b>{wd}:</b> \n {h(dish)}")

    text = "<b>Меню на неделю</b>:\n" + "\n\n".join(lines)

    # Клавиатура: кнопка «Назад»
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Назад к сегодняшнему меню", callback_data="show_menu_again")
    # редактируем текущее сообщение → текст
    await edit_to_text(call.message, text, reply_markup=kb.as_markup())
    await call.answer()


@router.callback_query(F.data == "addr_custom")
async def cb_addr_custom(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "awaiting_custom_address")

    # Кнопка «Назад» из режима ввода — вернёт на выбор адресов
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
        await bot.send_message(call.message.chat.id, "Пожалуйста, введи адрес доставки текстом одним сообщением.",
                               reply_markup=kb)
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

    qty = int(choice)  # 1 или 2
    await fsm.update_data(uid, qty=qty)
    await _show_confirm(uid, call.message)
    await call.answer()



@router.callback_query(F.data.startswith("support_reply:"))
async def cb_support_reply(call: CallbackQuery):
    # только админ
    if not ADMIN_CHAT_ID or call.from_user.id != ADMIN_CHAT_ID:
        return await call.answer("Нет прав", show_alert=True)

    target_user_id = int(call.data.split(":", 1)[1])

    # у админа в FSM сохраним, кому отвечаем
    await fsm.set_state(ADMIN_CHAT_ID, "awaiting_support_reply")
    await fsm.update_data(ADMIN_CHAT_ID, reply_target=target_user_id)

    # уберём кнопку у этого сообщения, чтобы не нажимали повторно
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await call.message.answer(
        f"Напишите ответ для пользователя <code>{target_user_id}</code> одним сообщением.",
        parse_mode="HTML"
    )
    await call.answer()

# ---------- Точка входа ----------

async def on_startup():
    await fsm.init()
    await asyncio.to_thread(get_sheets_client)

    # Команды, которые увидит пользователь в синей кнопке «Меню»
    await bot.set_my_commands([
        BotCommand(command="start", description="Начать / регистрация"),
        BotCommand(command="menu", description="Показать меню на сегодня"),
        BotCommand(command="support", description="Связаться с нами"),
        BotCommand(command="info", description="Инфо о доставке и оплате"),
    ])
    # Явно выставим тип меню как Commands (на всякий случай)
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception:
        pass


async def daily_930_broadcast_task():
    """
    Бесконечный цикл:
      - ждём до ближайших 09:30 МСК,
      - если в Меню есть позиции на сегодня — отправляем автопостинг всем клиентам,
      - повторяем.
    """
    await asyncio.sleep(3)  # дать боту подняться
    while True:
        try:
            delay = seconds_until_next_930_msk()
            await asyncio.sleep(delay)

            # собираем меню на сегодня
            today_marker = (msk_now_dt()).strftime("%Y-%m-%d %H:%M:%S")
            menu_items = await sheets_get_menu(today_marker)
            if not menu_items:
                # сегодня меню нет — просто пропускаем рассылку
                continue

            menu_text = format_menu_for_broadcast(menu_items)
            if not menu_text.strip():
                # нет осмысленных позиций
                continue

            templates = [
                "В меню:\n{menu}\n\nКоличество ограничено 😋",
                "Сегодня в меню:\n{menu}\n\nУспей заказать. Количество ограничено 😋",
                "Сегодня у нас в меню:\n{menu}\n\nУспей заказать 😋",
            ]
            text_to_send = random.choice(templates).format(menu=menu_text)

            # адресаты из листа "Клиенты"
            ids = await sheets_get_all_client_ids()
            if not ids:
                continue

            # аккуратно рассылаем
            for uid in ids:
                try:
                    await bot.send_message(uid, text_to_send)
                except Exception:
                    pass
                await asyncio.sleep(0.05)  # мягкий троттлинг
        except Exception:
            # чтобы цикл не умер на исключении
            await asyncio.sleep(5)


# Настраиваем логгер
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

async def main():
    logging.info(now_msk_str())
    # Временно отключить автопостинг = закомментировать строчку ниже
    asyncio.create_task(daily_930_broadcast_task())
    await on_startup()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
