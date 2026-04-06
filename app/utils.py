# -*- coding: utf-8 -*-
import html
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

WEEKDAYS_RU = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]


def normalize_phone(text: str) -> Optional[str]:
    """Простая валидация телефона: оставляем + и цифры, проверяем длину >= 10."""
    digits = re.sub(r"[^\d+]", "", text or "").strip()
    if digits.startswith("8"):
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

    m = re.search(r'(\b\d{2}\.\d{2}\.\d{4}\b)', s)
    if m:
        return m.group(1)

    m = re.search(r'\b(\d{4})-(\d{2})-(\d{2})\b', s)
    if m:
        y, mo, d = m.groups()
        return f"{d}.{mo}.{y}"

    return datetime.now().strftime("%d.%m.%Y")


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
    return WEEKDAYS_RU[dt.isoweekday() - 1]


def now_msk_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def msk_now_dt() -> datetime:
    return datetime.now()


def start_of_week_msk_dt() -> datetime:
    dt = msk_now_dt()
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


def h(s: str) -> str:
    return html.escape(s or "", quote=False)


def format_menu_for_broadcast(items: List[Dict[str, Any]]) -> str:
    lines = []
    for it in items:
        dish = str(it.get("Блюда", "")).strip()
        if dish:
            lines.append(f"{dish}")
    return "\n".join(lines)

