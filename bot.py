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
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatAction
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart
from aiogram.types import (
    CallbackQuery,
    Message,
    ContentType,
    ReplyKeyboardRemove,
    InputMediaPhoto,
    BufferedInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# ---------- Константы настройки ----------
ADDRESS_OPTIONS = ["Цельсий", "Катин Бор", "Дубровская"]
TIME_SLOTS = ["12-13", "13-14"]

DB_PATH = "fsm.sqlite3"

# ---------- Загрузка окружения ----------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
GSHEET_ID = os.getenv("GSHEET_ID")
SERVICE_ACCOUNT_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SERVICE_ACCOUNT_INFO = os.getenv("GOOGLE_SERVICE_ACCOUNT_INFO")  # альтернатива пути

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

def h(s: str) -> str:
    return html.escape(s or "", quote=False)

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
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        address: str,
        timeslot: str,
    ):
        ws = self.ws_orders()
        ws.append_row(
            [date_str, str(user_id), name, phone, dish, address, timeslot],
            value_input_option="USER_ENTERED",
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

        # проходим по строкам со второй (index=1) — это 2-я строка в таблице
        for i in range(1, len(all_values)):
            row = all_values[i]
            if len(row) <= max(day_col, dish_col):
                continue
            if str(row[day_col]).strip() == str(day_name).strip() and str(row[dish_col]).strip() == str(dish_name).strip():
                # собираем record по заголовкам (безопасно)
                record = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
                # индекс строки для gspread 1-based → i+1
                return i + 1, record
        return None, None

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

async def reserve_one_portion_for_today(dish_name: str) -> tuple[bool, str | None]:
    """
    Пытается уменьшить Количество на 1 для сегодняшнего дня и указанного блюда.
    Возвращает (True, None) если успешно, (False, "причина") если нет.
    """
    day = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_index, record = await sheets_find_menu_row(day, dish_name)
    if not row_index:
        return False, "Позиция меню не найдена."

    current = await sheets_get_quantity_by_row(row_index)
    if current <= 0:
        return False, "Увы, блюдо закончилось."

    # оптимистичное уменьшение
    await sheets_set_quantity_by_row(row_index, current - 1)
    return True, None

async def release_one_portion_for_today(dish_name: str):
    day = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_index, record = await sheets_find_menu_row(day, dish_name)
    if not row_index:
        return
    current = await sheets_get_quantity_by_row(row_index)
    # безопасно вернуть 1 (можем ограничить верхнюю границу по желанию)
    await sheets_set_quantity_by_row(row_index, current + 1)


async def sheets_append_order(
    user_id: int,
    name: str,
    phone: str,
    dish: str,
    address: str,
    timeslot: str,
):
    date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await asyncio.to_thread(
        get_sheets_client().append_order,
        date_str, user_id, name, phone, dish, address, timeslot
    )

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
router = Router()
dp.include_router(router)

# ---- Клавиатуры ----

def kb_send_contact():
    kb = ReplyKeyboardBuilder()
    kb.button(text="Поделиться контактом", request_contact=True)
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=True)

def kb_menu_navigation(can_switch: bool, show_choose: bool = True) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    if can_switch:
        kb.button(text="◀️", callback_data="menu_prev")
    if show_choose:
        kb.button(text="Выбрать", callback_data="menu_choose")
    if can_switch:
        kb.button(text="▶️", callback_data="menu_next")

    if can_switch and show_choose:
        kb.adjust(3)
    elif can_switch or show_choose:
        kb.adjust(2)
    else:
        kb.adjust(1)
    return kb

def kb_choose_address() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for a in ADDRESS_OPTIONS:
        kb.button(text=a, callback_data=f"addr:{a}")
    kb.button(text="Назад", callback_data="back:menu")
    kb.adjust(3, 1)
    return kb

def kb_choose_time() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    for t in TIME_SLOTS:
        kb.button(text=t, callback_data=f"time:{t}")
    kb.button(text="Назад", callback_data="back:addr")
    kb.adjust(2, 1)
    return kb

def kb_confirm() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Всё верно", callback_data="confirm")
    kb.button(text="Назад", callback_data="back:time")
    kb.adjust(1, 1)
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
        await message.answer("Привет! Давай познакомимся. Как тебя зовут? 🙂")

async def send_today_menu(chat_id: int, user_id: int):
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

    caption_lines = [f"<b>Блюдо дня</b>: {h(dish_name)}"]
    if qty:
        caption_lines.append(f"Доступно: {h(qty)}")
    caption = "\n".join(caption_lines)

    can_switch = len(menu) > 1
    kb = kb_menu_navigation(can_switch=can_switch, show_choose=True).as_markup()

    # ✅ chat_action: если есть фото — UPLOAD_PHOTO, иначе TYPING
    try:
        await bot.send_chat_action(chat_id, ChatAction.UPLOAD_PHOTO if photo else ChatAction.TYPING)
    except Exception:
        pass

    # дальше — твоя логика отправки/редактирования (из предыдущей версии)
    if not callback_query:
        await _safe_send_photo_or_text(chat_id, photo, caption, kb)
        return

    msg = callback_query.message
    try:
        if photo:
            if msg.photo:
                ok = await _edit_media_smart(msg, photo, caption, kb)
                if not ok:
                    try:
                        await bot.delete_message(msg.chat.id, msg.message_id)
                    except Exception:
                        pass
                    await _safe_send_photo_or_text(chat_id, photo, caption, kb)
            else:
                try:
                    await bot.delete_message(msg.chat.id, msg.message_id)
                except Exception:
                    pass
                await _safe_send_photo_or_text(chat_id, photo, caption, kb)
        else:
            if msg.photo:
                try:
                    await bot.delete_message(msg.chat.id, msg.message_id)
                except Exception:
                    pass
                await bot.send_message(chat_id, caption, reply_markup=kb, disable_web_page_preview=True)
            else:
                try:
                    await msg.edit_text(caption, reply_markup=kb, disable_web_page_preview=True)
                except TelegramBadRequest as e:
                    if "message is not modified" in str(e).lower():
                        pass
                    else:
                        await bot.send_message(chat_id, caption, reply_markup=kb, disable_web_page_preview=True)
    finally:
        if callback_query:
            await callback_query.answer()


# ---------- Хэндлеры ----------

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

    # Ожидаем имя при регистрации
    if state == "awaiting_name":
        name = message.text.strip()
        if len(name) < 2:
            await message.answer("Имя должно быть длиннее. Попробуй снова, пожалуйста.")
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
    await message.answer("Спасибо! Регистрация завершена ✅", reply_markup=None)
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


@router.callback_query(F.data == "menu_choose")
async def cb_menu_choose(call: CallbackQuery):
    uid = call.from_user.id
    data = await fsm.get_data(uid)
    menu = data.get("menu", [])
    if not menu:
        return await call.answer("Меню пустое.")

    idx = data.get("menu_idx", 0)
    dish = str(menu[idx].get("Блюда", "")).strip()

    # Проверка остатков (без списания)
    day = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row_index, _ = await sheets_find_menu_row(day, dish)
    if not row_index:
        return await call.answer("Позиция меню не найдена.", show_alert=True)
    qty = await sheets_get_quantity_by_row(row_index)
    if qty <= 0:
        return await call.answer("Увы, это блюдо уже закончилось.", show_alert=True)

    # Сохраняем выбор
    await fsm.update_data(uid, chosen_dish=dish)
    await fsm.set_state(uid, "choose_address")

    # Клавиатура и ТЕКСТ
    kb = kb_choose_address().as_markup()
    text = f"Вы выбрали: <b>{h(dish)}</b>\n\nВыбери адрес доставки:"

    # ❗️Всегда удаляем карточку (фото/текст) и шлём НОВОЕ текстовое сообщение
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
    await fsm.set_state(uid, "confirm")

    data = await fsm.get_data(uid)
    dish = data.get("chosen_dish", "")
    address = data.get("chosen_address", "")
    kb = kb_confirm().as_markup()

    text = (
        "Проверь заказ:\n"
        f"• Блюдо: <b>{h(dish)}</b>\n"
        f"• Адрес: <b>{h(address)}</b>\n"
        f"• Время: <b>{h(timeslot)}</b>\n\n"
        "Подтвердить заказ?"
    )

    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        try:
            await bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        await bot.send_message(call.message.chat.id, text, reply_markup=kb, parse_mode="HTML")

    await call.answer()

@router.callback_query(F.data == "confirm")
async def cb_confirm(call: CallbackQuery):
    user = call.from_user
    if not user:
        return
    uid = user.id

    data = await fsm.get_data(uid)
    dish = data.get("chosen_dish")
    address = data.get("chosen_address")
    timeslot = data.get("chosen_time")

    # Повторная проверка и «резерв» 1 порции
    ok, err = await reserve_one_portion_for_today(dish)
    if not ok:
        await call.answer(err or "Не удалось оформить: блюдо закончилось.", show_alert=True)
        await fsm.set_state(uid, "menu")
        await send_today_menu(call.message.chat.id, uid)
        return

    # Клиент
    client = await sheets_find_client(uid)
    if not client:
        await release_one_portion_for_today(dish)  # откат резерва
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

    # Запись заказа в "Заказы"
    try:
        await sheets_append_order(uid, name, phone, dish, address, timeslot)
    except Exception:
        await release_one_portion_for_today(dish)  # откат
        await call.answer("Не удалось сохранить заказ. Попробуй позже.", show_alert=True)
        return

    # Финальный текст + кнопка "Посмотреть меню на сегодня"
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
        text = f"Выбранное блюдо: <b>{dish}</b>\n\nВыбери адрес доставки:"
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

    # fallback
    await fsm.set_state(uid, "menu")
    await show_menu_item(call.message.chat.id, uid, callback_query=call)


@router.callback_query(F.data == "show_menu_again")
async def cb_show_menu_again(call: CallbackQuery):
    uid = call.from_user.id
    await fsm.set_state(uid, "menu")
    await send_today_menu(call.message.chat.id, uid)
    await call.answer()


# ---------- Точка входа ----------

async def on_startup():
    await fsm.init()
    # Прогреем клиент Google Sheets в отдельном потоке
    await asyncio.to_thread(get_sheets_client)

async def main():
    await on_startup()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
