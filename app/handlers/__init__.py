# -*- coding: utf-8 -*-
from aiogram import Dispatcher

from .callbacks import router as callbacks_router
from .commands import router as commands_router
from .messages import router as messages_router


def register_all_handlers(dp: Dispatcher):
    dp.include_router(commands_router)
    dp.include_router(messages_router)
    dp.include_router(callbacks_router)

