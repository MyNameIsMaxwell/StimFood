# -*- coding: utf-8 -*-
import json
import os
from typing import List

from dotenv import load_dotenv

# Load environment variables early
load_dotenv()

# Constants and environment configuration
ADDRESS_OPTIONS: List[str] = ["Цельсий", "Дубровская(СТиМ)", "Катин Бор(СТиМ)", "БОНШЕ"]
TIME_SLOTS: List[str] = ["12-13", "13-14"]

# Tariff prices (per day) and subscription discounts (by duration)
# Discounts: fraction to subtract (e.g., 0.05 == 5%)
TARIFF_PRICES: dict[str, int] = {
    "Максимум": 16,
    "Стандарт+": 14,
    "Стандарт (+суп)": 12,
    "Стандарт (+салат)": 12,
}
# Pricing mode: "free_days" (бесплатные дни) или "percent" (скидка в долях)
SUBSCRIPTION_PRICING_MODE: str = os.getenv("SUBSCRIPTION_PRICING_MODE", "free_days")

# Скидки в процентах (используются, если SUBSCRIPTION_PRICING_MODE == "percent")
SUBSCRIPTION_DISCOUNTS: dict[int, float] = {
    5: 0.0,
    10: 0.05,
    20: 0.10,
}

# Бесплатные дни (используются, если SUBSCRIPTION_PRICING_MODE == "free_days")
# Пример: 10 дней -> 1 бесплатно (платим за 9), 20 дней -> 2 бесплатно (платим за 18)
SUBSCRIPTION_FREE_DAYS: dict[int, int] = {
    5: 0,
    10: 0,
    20: 0,
}

DB_PATH: str = "fsm.sqlite3"

BOT_TOKEN: str | None = os.getenv("BOT_TOKEN")
GSHEET_ID: str | None = os.getenv("GSHEET_ID")
SERVICE_ACCOUNT_JSON_PATH: str | None = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SERVICE_ACCOUNT_INFO: str | None = os.getenv("GOOGLE_SERVICE_ACCOUNT_INFO")  # alternative to file path
ADMIN_CHAT_ID: int = int(os.getenv("ADMIN_CHAT_ID", "0") or 0)

# Optional CRM configuration
CRM_ENABLED: str | None = os.getenv("CRM_ENABLED")
CRM_ENDPOINT: str | None = os.getenv("CRM_ENDPOINT")
CRM_IDENTIFIER: str | None = os.getenv("CRM_IDENTIFIER")
CRM_WEBAPI_KEY: str | None = os.getenv("CRM_WEBAPI_KEY")

# Additional recipients to notify (comma-separated integers)
_raw_notify = os.getenv("NOTIFY_IDS", "") or ""
NOTIFY_IDS: List[int] = []
for part in _raw_notify.split(","):
    p = part.strip()
    if p.isdigit():
        try:
            NOTIFY_IDS.append(int(p))
        except Exception:
            pass

# Combined set of all admin user IDs (ADMIN_CHAT_ID + NOTIFY_IDS)
ADMIN_IDS: set[int] = set()
if ADMIN_CHAT_ID:
    ADMIN_IDS.add(ADMIN_CHAT_ID)
for _nid in NOTIFY_IDS:
    ADMIN_IDS.add(_nid)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# Validate required configuration early to fail-fast
if not BOT_TOKEN or not GSHEET_ID or not (SERVICE_ACCOUNT_JSON_PATH or SERVICE_ACCOUNT_INFO):
    raise RuntimeError(
        "Отсутствуют переменные окружения BOT_TOKEN, GSHEET_ID "
        "и GOOGLE_SERVICE_ACCOUNT_JSON (или GOOGLE_SERVICE_ACCOUNT_INFO)."
    )

