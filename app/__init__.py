# -*- coding: utf-8 -*-
import logging

from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties

from app.config import BOT_TOKEN
from app.fsm import FSMStorage

# Configure logging similarly to the original script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Shared instances
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
fsm = FSMStorage()

# Default routers registry point if needed by submodules
root_router = Router()
dp.include_router(root_router)

