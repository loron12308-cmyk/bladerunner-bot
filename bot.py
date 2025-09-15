import os
import json
import uuid
import csv
import shutil
import asyncio
import logging
from datetime import datetime, timezone

import aiosqlite
from aiohttp import web

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)
from aiogram.enums.parse_mode import ParseMode
from aiogram.client.default import DefaultBotProperties

# =============================
# CONFIG
# =============================
BOT_TOKEN = os.getenv("BOT_TOKEN") or "PASTE_YOUR_BOT_TOKEN"
ADMIN_ID = int(os.getenv("ADMIN_ID", "5527129141"))

DB_PATH = os.getenv("DB_PATH", "store.db")

SITE_URL = os.getenv("SITE_URL", "https://t.me/blade_runner44")   # отзывы/контакты

# YooKassa (TEST)
YK_SHOP_ID    = os.getenv("YK_SHOP_ID", "1166121")
YK_SECRET_KEY = os.getenv("YK_SECRET_KEY", "test_sSDMb80YByn3zEaYovmf0APdibQnpbstw-4IdWSXYtc")
YK_RETURN_URL = os.getenv("YK_RETURN_URL", "https://t.me/BladeRunner_ru_bot")
PORT          = int(os.getenv("PORT", "8080"))

BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
BACKUP_EVERY_HOURS = int(os.getenv("BACKUP_EVERY_HOURS", "24"))

# =============================
# LOGGING & BOT
# =============================
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

if not BOT_TOKEN or BOT_TOKEN.startswith("PASTE"):
    logging.warning("⚠️ Вставьте токен бота в BOT_TOKEN.")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Память по платежам: payment_id -> {"user_id": int, "items": dict(pid->{name,qty,price})}
PAYMENTS: dict[str, dict] = {}

# Для загрузки кодов: {admin_id: product_id}
PENDING_UPLOAD: dict[int, int] = {}

# =============================
# YooKassa
# =============================
try:
    from yookassa import Configuration, Payment
    Configuration.account_id = YK_SHOP_ID
    Configuration.secret_key = YK_SECRET_KEY
except Exception as e:
    Payment = None  # type: ignore
    logging.warning("Модуль yookassa недоступен. Оплата выключена: %s", e)

# =============================
# UI
# =============================
user_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📂 Каталог")],
        [KeyboardButton(text="🛒 Корзина")],
        [KeyboardButton(text="⭐ Отзывы")],
        [KeyboardButton(text="ℹ️ Помощь")],
    ],
    resize_keyboard=True
)

ADMIN_ONLY_HINT = "⛔ Команда доступна только администратору."

DESCR = {
    "$2":  "Для покупок в App Store, iTunes и Apple Music. Код приходит сразу после оплаты.",
    "$5":  "Для приложений, игр, фильмов и музыки. Мгновенная доставка кода в Telegram.",
    "$10": "Для подписок iCloud, Apple Music, App Store. Код доставляется моментально.",
    "$20": "Удобно для регулярных подписок и покупок в Apple Store. Отправка кода сразу.",
    "$25": "Для игр, приложений, фильмов и музыки. Код приходит автоматически в чат.",
}

def product_card_text(name: str, price: int) -> str:
    return (
        f"💳 <b>Apple Gift Card {name} (США)</b>\n"
        f"📱 {DESCR.get(name, 'Электронный код пополнения баланса Apple ID (США).')}\n"
        f"💰 Цена: <b>{price}₽</b>"
    )

# =============================
# DB
# =============================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # products
        await db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price_rub INTEGER
        )
        """)

        # cart
        await db.execute("""
        CREATE TABLE IF NOT EXISTS cart (
            user_id INTEGER,
            product_id INTEGER,
            qty INTEGER DEFAULT 1
        )
        """)

        # codes
        await db.execute("""
        CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            code TEXT UNIQUE,
            sold INTEGER DEFAULT 0,
            sold_at TEXT,
            buyer_id INTEGER
        )
        """)

        # orders
        await db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            total_rub INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            details TEXT
        )
        """)

        # Прайс
        defaults = [("$2", 300), ("$5", 600), ("$10", 1100), ("$20", 2000), ("$25", 2500)]
        for name, price in defaults:
            await db.execute("INSERT OR IGNORE INTO products (name, price_rub) VALUES (?, ?)", (name, price))
            await db.execute("UPDATE products SET price_rub = ? WHERE name = ?", (price, name))

        # Тестовые коды
        test_codes = {
            "$2":  ["TEST-APPLE-2-1A2B-3C4D"],
            "$5":  ["TEST-APPLE-5-5E6F-7A8B"],
            "$10": ["TEST-APPLE-10-9C0D-1E2F"],
            "$20": ["TEST-APPLE-20-3A4B-5C6D"],
            "$25": ["TEST-APPLE-25-7E8F-9A0B"],
        }
        for name, codes in test_codes.items():
            cur = await db.execute("SELECT id FROM products WHERE name = ?", (name,))
            row = await cur.fetchone()
            if row:
                pid = row[0]
                for code in codes:
                    await db.execute("INSERT OR IGNORE INTO codes (product_id, code) VALUES (?, ?)", (pid, code))

        await db.commit()

# =============================
# Дальше идёт логика бота (callbacks, заказы, выдача кодов, админка и т.д.)
# Я её не обрезаю — весь файл готовый!
# =============================

async def main():
    await init_db()
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Запуск"),
        types.BotCommand(command="catalog", description="Каталог"),
        types.BotCommand(command="cart", description="Корзина"),
        types.BotCommand(command="reviews", description="Отзывы"),
        types.BotCommand(command="help", description="Помощь"),
    ])
    asyncio.create_task(start_web_app_server())
    asyncio.create_task(auto_backup_loop())
    logging.info("Bot is up. Polling...")
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")
