# -*- coding: utf-8 -*-
from aiogram import Router, F
from aiogram.types import CallbackQuery, PollAnswer, InputPollOption
import asyncio
import requests
from aiohttp import ClientError

from app import bot, fsm
from app.config import is_admin
from app.polls import get_poll_store, record_poll_answer
from app.sheets import sheets_get_all_client_ids

router = Router()


@router.callback_query(F.data == "poll_cancel")
async def cb_poll_cancel(call: CallbackQuery):
    admin_uid = call.from_user.id
    if not is_admin(admin_uid):
        return await call.answer("Нет прав", show_alert=True)
    await fsm.set_state(admin_uid, None)
    await fsm.update_data(admin_uid, poll_question=None, poll_options=None)
    await call.message.edit_text("Опрос отменён.")
    await call.answer()


@router.callback_query(F.data == "poll_send")
async def cb_poll_send(call: CallbackQuery):
    admin_uid = call.from_user.id
    if not is_admin(admin_uid):
        return await call.answer("Нет прав", show_alert=True)
    data = await fsm.get_data(admin_uid)
    question = (data.get("poll_question") or "").strip()
    options = data.get("poll_options") or []
    if not question or len(options) < 2:
        await call.answer("Вопрос или варианты не заданы.", show_alert=True)
        return
    store = get_poll_store()
    await store.init_tables()
    batch_id = await store.create_batch(question, options, created_by=admin_uid)
    try:
        ids = await sheets_get_all_client_ids()
    except (ClientError, requests.exceptions.RequestException, Exception):
        await call.message.edit_text("Не удалось получить список получателей из Google Sheets. Попробуйте позже.")
        await call.answer()
        return
    if not ids:
        await call.message.edit_text("В листе «Клиенты» нет получателей или таблица временно недоступна.")
        await call.answer()
        return
    poll_options = [InputPollOption(text=o) for o in options]
    sent, fail = 0, 0
    for uid in ids:
        try:
            msg = await bot.send_poll(
                chat_id=uid,
                question=question,
                options=poll_options,
                is_anonymous=False,
                allows_multiple_answers=False
            )
            if msg and msg.poll and msg.poll.id:
                await store.add_poll_id(msg.poll.id, batch_id=batch_id, chat_id=uid)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            fail += 1
    await fsm.set_state(admin_uid, None)
    await fsm.update_data(admin_uid, poll_question=None, poll_options=None)
    await call.message.edit_text(f"✅ Рассылка опроса завершена.\nУспешно: {sent}\nОшибок: {fail}")
    await call.answer()


@router.poll_answer()
async def on_poll_answer(poll_answer: PollAnswer):
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    selected = poll_answer.option_ids or []
    store = get_poll_store()
    ref = await store.get_batch_by_poll_id(poll_id)
    if not ref:
        return
    _, question, options, _ = ref
    answer_text = ""
    if selected and 0 <= selected[0] < len(options):
        answer_text = str(options[selected[0]])
    if not answer_text:
        return
    await record_poll_answer(user.id, question, answer_text)
