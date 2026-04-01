# -*- coding: utf-8 -*-
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

from app.config import GSHEET_ID, SERVICE_ACCOUNT_INFO, SERVICE_ACCOUNT_JSON_PATH
from app.utils import extract_ddmmyyyy, now_msk_str, parse_ddmmyyyy
import asyncio


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

    # Sheets
    def ws_clients(self):
        return self._sh.worksheet("Клиенты")

    def ws_menu(self):
        return self._sh.worksheet("Меню")

    def ws_orders(self):
        return self._sh.worksheet("Заказы")

    def ws_overorders(self):
        return self._sh.worksheet("Заказы свыше")

    # Clients
    def find_client(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        ws = self.ws_clients()
        records = ws.get_all_records()
        for row in records:
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

    # Menu
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
            rec = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
            cell_day = extract_ddmmyyyy(str(rec.get("День", "")))
            if cell_day == want_day:
                items.append(rec)
        return items

    def find_menu_row_by_day_and_dish(self, day_name: str, dish_name: str) -> Tuple[int | None, dict | None]:
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

        for i in range(1, len(all_values)):
            row = all_values[i]
            if len(row) <= max(day_col, dish_col):
                continue
            cell_day_raw = str(row[day_col]).strip()
            cell_dish = str(row[dish_col]).strip().lower()
            cell_day = extract_ddmmyyyy(cell_day_raw)
            if cell_day == want_day and cell_dish == want_dish:
                record = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
                return i + 1, record
        return None, None

    def get_quantity_by_row(self, row_index: int) -> int:
        ws = self.ws_menu()
        headers = ws.row_values(1)
        qty_col = headers.index("Количество") + 1
        val = ws.cell(row_index, qty_col).value
        try:
            return int(str(val).strip())
        except Exception:
            return 0

    def set_quantity_by_row(self, row_index: int, new_qty: int):
        ws = self.ws_menu()
        headers = ws.row_values(1)
        qty_col = headers.index("Количество") + 1
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
        items.sort(key=lambda r: parse_ddmmyyyy(str(r.get("День", ""))) or datetime.now())
        return items

    # Orders
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
        payment_label: str,
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

    def append_overorder(self, date_str: str, user_id: int, name: str, phone: str, dish: str, address: str, timeslot: str):
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


_sheets_client: Optional[GoogleSheetsClient] = None


def get_sheets_client() -> GoogleSheetsClient:
    global _sheets_client
    if _sheets_client is None:
        _sheets_client = GoogleSheetsClient(GSHEET_ID)
    return _sheets_client


# Async wrappers
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
    await asyncio.to_thread(
        get_sheets_client().append_overorder,
        date_str, user_id, name, phone, dish, "", ""
    )


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


async def reserve_portions_for_today(dish_name: str, qty: int) -> tuple[bool, str | None]:
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

