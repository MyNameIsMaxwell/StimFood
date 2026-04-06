# -*- coding: utf-8 -*-
import logging
from aiohttp import ClientSession, ClientError

from app.config import CRM_ENABLED, CRM_ENDPOINT, CRM_IDENTIFIER, CRM_WEBAPI_KEY


async def crm_send_order(
    user_id: int,
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
        str(dish or ""),
        str(qty or ""),
        str(timeslot or ""),
        str(payment_label or ""),
    ]).strip()

    payload = {
        "identifier": CRM_IDENTIFIER,
        "webApiKey": CRM_WEBAPI_KEY,
        "user_id": str(user_id),
        "name": name,
        "phone": phone,
        "address_text": address,
        "is_retail_order": 1,
        "comment": comment,
    }

    try:
        async with ClientSession() as session:
            async with session.post(CRM_ENDPOINT, data=payload, timeout=20) as resp:
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

