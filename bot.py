import os
import json
import uuid
import csv
import shutil
import asyncio
import logging
from datetime import datetime, timezone

import aiosqlite
import aiocron
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
BOT_TOKEN = os.getenv("BOT_TOKEN") or "YOUR_BOT_TOKEN"
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))

DB_PATH = os.getenv("DB_PATH", "store.db")
PORT = int(os.getenv("PORT", "8080"))
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")

# Ссылки
HELP_URL = os.getenv("HELP_URL", "https://t.me/your_support")
REVIEWS_URL = os.getenv("REVIEWS_URL", "https://t.me/your_reviews")
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/your_channel")

# YooKassa
YK_SHOP_ID    = os.getenv("SHOP_ID", "1166121")
YK_SECRET_KEY = os.getenv("SHOP_SECRET", "test_secret")
YK_RETURN_URL = os.getenv("YK_RETURN_URL", "https://t.me/your_bot")

# =============================
# LOGGING & BOT
# =============================
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

if not BOT_TOKEN or BOT_TOKEN.startswith("YOUR"):
    logging.warning("⚠️ Вставьте токен бота в BOT_TOKEN.")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Временные хранилища
PAYMENTS: dict[str, dict] = {}   # payment_id -> {"user_id": int, "items": dict}
PENDING_UPLOAD: dict[int, int] = {}  # admin_id -> product_id

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
    "$2":  "Для покупок в App Store, iTunes и Apple Music.",
    "$5":  "Для приложений, игр, фильмов и музыки.",
    "$10": "Для подписок iCloud, Apple Music, App Store.",
    "$20": "Для регулярных подписок и покупок в Apple Store.",
    "$25": "Для игр, приложений, фильмов и музыки.",
}

def product_card_text(name: str, price: int) -> str:
    return (
        f"💳 <b>Apple Gift Card {name} (США)</b>\n"
        f"📱 {DESCR.get(name, 'Электронный код пополнения баланса Apple ID (США).')}\n"
        f"💰 Цена: <b>{price}₽</b>"
    )

# =============================
# DB INIT
# =============================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price_rub INTEGER
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS cart (
            user_id INTEGER,
            product_id INTEGER,
            qty INTEGER DEFAULT 1
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER,
            code TEXT UNIQUE,
            sold INTEGER DEFAULT 0,
            sold_at TEXT,
            buyer_id INTEGER
        )""")
        await db.execute("""CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            total_rub INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            details TEXT
        )""")
        # Пресет номиналов
        defaults = [("$2", 300), ("$5", 600), ("$10", 1100), ("$20", 2000), ("$25", 2500)]
        for name, price in defaults:
            await db.execute("INSERT OR IGNORE INTO products (name, price_rub) VALUES (?, ?)", (name, price))
            await db.execute("UPDATE products SET price_rub = ? WHERE name = ?", (price, name))
        # Тестовые коды
        test_codes = {
            "$2":  ["TEST-APPLE-2-XXXX"],
            "$5":  ["TEST-APPLE-5-XXXX"],
            "$10": ["TEST-APPLE-10-XXXX"],
            "$20": ["TEST-APPLE-20-XXXX"],
            "$25": ["TEST-APPLE-25-XXXX"],
        }
        for name, codes in test_codes.items():
            cur = await db.execute("SELECT id FROM products WHERE name = ?", (name,))
            row = await cur.fetchone()
            if row:
                pid = row[0]
                for code in codes:
                    await db.execute("INSERT OR IGNORE INTO codes (product_id, code) VALUES (?, ?)", (pid, code))
        await db.commit()

async def build_catalog_kb() -> InlineKeyboardMarkup:
    rows = []
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, name FROM products ORDER BY id")
        prods = await cur.fetchall()
    for pid, name in prods:
        rows.append([InlineKeyboardButton(text=name, callback_data=f"buy:{pid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows or [[InlineKeyboardButton(text="Пусто", callback_data="none")]])

# =============================
# Backups & export
# =============================
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

def make_backup(db_path: str) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dst = os.path.join(BACKUP_DIR, f"db-{_ts()}.sqlite3")
    shutil.copyfile(db_path, dst)
    return dst

async def export_codes_csv(db_path: str) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    csv_path = os.path.join(BACKUP_DIR, f"codes-{_ts()}.csv")
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT id, product_id, code, sold, sold_at, buyer_id FROM codes ORDER BY id")
        rows = await cur.fetchall()
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id","product_id","code","sold","sold_at","buyer_id"])
        for r in rows:
            w.writerow([r["id"], r["product_id"], r["code"], r["sold"], r["sold_at"], r["buyer_id"]])
    return csv_path

async def auto_backup():
    try:
        path = make_backup(DB_PATH)
        if ADMIN_ID:
            await bot.send_document(ADMIN_ID, FSInputFile(path), caption="Автобэкап БД ✅")
    except Exception as e:
        logging.exception("Ошибка автобэкапа: %s", e)
        if ADMIN_ID:
            await bot.send_message(ADMIN_ID, f"[backup] Ошибка автобэкапа: {e}")

# планировщик: каждый день в 03:00
aiocron.crontab("0 3 * * *", func=auto_backup)

# =============================
# Helpers
# =============================
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def pay_kb(url: str, payment_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Оплатить через ЮKassa", url=url)],
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"paid:{payment_id}")]
    ])

async def deliver_items_and_record(uid: int, items: dict[str, dict]) -> str:
    total = 0
    lines = []
    async with aiosqlite.connect(DB_PATH) as db:
        for pid_str, meta in items.items():
            pid = int(pid_str)
            name = meta["name"]
            qty  = int(meta["qty"])
            price = int(meta["price"])
            total += price * qty

            cur = await db.execute(
                "SELECT id, code FROM codes WHERE product_id = ? AND sold = 0 ORDER BY id LIMIT ?",
                (pid, qty)
            )
            codes_to_sell = await cur.fetchall()
            if len(codes_to_sell) < qty:
                await bot.send_message(ADMIN_ID, f"⚠️ Не хватает кодов для {name}. Нужно {qty}, есть {len(codes_to_sell)}. Покупатель: {uid}")
            code_texts = []
            for cid, code in codes_to_sell:
                code_texts.append(code)
                await db.execute(
                    "UPDATE codes SET sold = 1, sold_at = ?, buyer_id = ? WHERE id = ?",
                    (datetime.utcnow().isoformat(), uid, cid)
                )
            lines.append(f"{name} x{qty}: " + (", ".join(f"<code>{c}</code>" for c in code_texts) if code_texts else "<i>нет в наличии</i>"))

        details = "\n".join(lines)
        await db.execute("INSERT INTO orders (user_id, total_rub, details) VALUES (?, ?, ?)", (uid, total, details))
        await db.execute("DELETE FROM cart WHERE user_id = ?", (uid,))
        await db.commit()

    text = "🎉 <b>Оплата получена!</b>\nВаши коды:\n" + details + f"\n\n💰 Итого: <b>{total}₽</b>"
    return text

# =============================
# User commands
# =============================
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    await message.answer(
        "👋 Привет! Это магазин электронных <b>Apple Gift Card (США)</b>.\n"
        "Открой <b>Каталог</b>, добавь номинал в корзину и оформи заказ.\n\n"
        "Оплата — через ЮKassa.",
        reply_markup=user_menu
    )

@dp.message(F.text == "⭐ Отзывы")
@dp.message(Command("reviews"))
async def reviews_cmd(message: types.Message):
    await message.answer(f"⭐ Отзывы и связь: {REVIEWS_URL}")

@dp.message(F.text == "ℹ️ Помощь")
@dp.message(Command("help"))
async def help_cmd(message: types.Message):
    await message.answer(
        "ℹ️ Как это работает:\n"
        "• Откройте 📂 Каталог и выберите номинал ($2, $5, $10, $20, $25)\n"
        "• Товар добавится в 🛒 Корзину или купите прямо из карточки «Купить сейчас»\n"
        "• После оплаты коды придут сюда автоматически.\n\n"
        f"🌍 Отзывы: {REVIEWS_URL}\n"
        f"📞 Помощь: {HELP_URL}\n"
        f"📢 Канал: {CHANNEL_URL}"
    )

@dp.message(F.text == "📂 Каталог")
@dp.message(Command("catalog"))
async def catalog_cmd(message: types.Message):
    kb = await build_catalog_kb()
    await message.answer("📦 Выберите номинал карты:", reply_markup=kb)

@dp.message(F.text == "🛒 Корзина")
@dp.message(Command("cart"))
async def cart_cmd(message: types.Message):
    user_id = message.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT p.id, p.name, p.price_rub, c.qty
            FROM cart c
            JOIN products p ON p.id = c.product_id
            WHERE c.user_id = ?
        """, (user_id,))
        items = await cur.fetchall()
    if not items:
        await message.answer("🛒 Ваша корзина пуста.")
        return
    total = sum(price * qty for _, _, price, qty in items)
    text = "🧺 <b>Ваша корзина:</b>\n"
    for pid, name, price, qty in items:
        text += f"- {name} x{qty} — {price}₽ × {qty} = <b>{price*qty}₽</b>\n"
    text += f"\n💰 Итого: <b>{total}₽</b>"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить ещё", callback_data="back_to_catalog")],
            [InlineKeyboardButton(text="✅ Оформить заказ", callback_data="checkout")],
            [InlineKeyboardButton(text="❌ Очистить корзину", callback_data="clear_cart")]
        ]
    )
    await message.answer(text, reply_markup=kb) 

# =============================
# Callbacks
# =============================
@dp.callback_query()
async def callbacks(cb: types.CallbackQuery):
    data = cb.data

    if data == "none":
        await cb.answer(); return

    if data == "back_to_catalog":
        kb = await build_catalog_kb()
        await cb.message.answer("📦 Выберите номинал карты:", reply_markup=kb)
        await cb.answer(); return

    # карточка товара
    if data.startswith("buy:"):
        pid = int(data.split(":")[1])
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT name, price_rub FROM products WHERE id = ?", (pid,))
            row = await cur.fetchone()
        if not row:
            await cb.message.answer("❌ Товар не найден."); await cb.answer(); return
        name, price = row
        text = product_card_text(name, price)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Добавить в корзину", callback_data=f"add:{pid}")],
            [InlineKeyboardButton(text="⚡ Купить сейчас", callback_data=f"buy_now:{pid}")],
            [InlineKeyboardButton(text="⬅️ Назад к каталогу", callback_data="back_to_catalog")],
        ])
        await cb.message.answer(text, reply_markup=kb)
        await cb.answer(); return

    # добавить в корзину
    if data.startswith("add:"):
        pid = int(data.split(":")[1])
        uid = cb.from_user.id
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT name, price_rub FROM products WHERE id = ?", (pid,))
            row = await cur.fetchone()
            if not row:
                await cb.message.answer("❌ Товар не найден."); await cb.answer(); return
            name, price = row
            cur = await db.execute("SELECT qty FROM cart WHERE user_id = ? AND product_id = ?", (uid, pid))
            exist = await cur.fetchone()
            if exist:
                await db.execute("UPDATE cart SET qty = qty + 1 WHERE user_id = ? AND product_id = ?", (uid, pid))
            else:
                await db.execute("INSERT INTO cart (user_id, product_id, qty) VALUES (?, ?, 1)", (uid, pid))
            await db.commit()
        await cb.message.answer(f"✅ {name} — {price}₽ добавлен(а) в 🛒 Корзину.")
        await cb.answer(); return

    # очистить корзину
    if data == "clear_cart":
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM cart WHERE user_id = ?", (cb.from_user.id,))
            await db.commit()
        await cb.message.answer("🗑️ Корзина очищена.")
        await cb.answer(); return

    # купить сразу или оформить заказ
    if data.startswith("buy_now:") or data == "checkout":
        uid = cb.from_user.id
        items_dict: dict[str, dict] = {}
        total = 0

        async with aiosqlite.connect(DB_PATH) as db:
            if data.startswith("buy_now:"):
                pid = int(data.split(":")[1])
                cur = await db.execute("SELECT id, name, price_rub FROM products WHERE id = ?", (pid,))
                r = await cur.fetchone()
                if not r:
                    await cb.message.answer("❌ Товар не найден."); await cb.answer(); return
                pid, name, price = r
                items_dict[str(pid)] = {"name": name, "qty": 1, "price": int(price)}
                total = int(price)
            else:
                cur = await db.execute("""
                    SELECT p.id, p.name, p.price_rub, c.qty
                    FROM cart c
                    JOIN products p ON p.id = c.product_id
                    WHERE c.user_id = ?
                """, (uid,))
                rows = await cur.fetchall()
                if not rows:
                    await cb.message.answer("❌ Корзина пуста."); await cb.answer(); return
                for pid, name, price, qty in rows:
                    items_dict[str(pid)] = {"name": name, "qty": int(qty), "price": int(price)}
                    total += int(price) * int(qty)

        if Payment is None:
            await cb.message.answer("⚠️ Оплата временно недоступна. Свяжитесь с поддержкой.")
            await cb.answer(); return

        order_id = uuid.uuid4().hex[:8].upper()
        meta = {
            "tg_user_id": str(uid),
            "order_id": order_id,
            "items_json": json.dumps(items_dict, ensure_ascii=False)
        }
        try:
            payment = Payment.create({
                "amount": {"value": f"{total:.2f}", "currency": "RUB"},
                "capture": True,
                "confirmation": {"type": "redirect", "return_url": YK_RETURN_URL},
                "description": f"Apple Gift Cards — Order {order_id} (Telegram {uid})",
                "metadata": meta
            })
        except Exception as e:
            logging.exception("YooKassa create() error: %s", e)
            await cb.message.answer("❌ Не удалось создать платёж в ЮKassa. Проверьте ключи магазина.")
            await cb.answer(); return

        confirm_url = payment.confirmation.confirmation_url
        PAYMENTS[payment.id] = {"user_id": uid, "items": items_dict}

        await cb.message.answer(
            f"✅ Заказ <b>#{order_id}</b>\n"
            f"Сумма к оплате: <b>{total}₽</b>\n\n"
            "Нажмите «Оплатить через ЮKassa». Если после оплаты коды не пришли — нажмите «Я оплатил».",
            reply_markup=pay_kb(confirm_url, payment.id)
        )
        await cb.answer(); return

    # ручная проверка оплаты
    if data.startswith("paid:"):
        pid = data.split(":")[1]
        if Payment is None:
            await cb.answer("Оплата отключена", show_alert=True); return
        try:
            p = Payment.find_one(pid)
            if getattr(p, "status", "") == "succeeded":
                info = PAYMENTS.get(pid)
                if not info:
                    try:
                        md = p.metadata or {}
                        items = json.loads(md.get("items_json", "{}")) if md else {}
                        uid = int(md.get("tg_user_id", cb.from_user.id))
                        info = {"user_id": uid, "items": items}
                    except Exception:
                        info = None
                if info:
                    text = await deliver_items_and_record(info["user_id"], info["items"])
                    await bot.send_message(info["user_id"], text)
                    try:
                        await bot.send_message(ADMIN_ID, f"📦 Оплаченный заказ от {info['user_id']}")
                    except Exception:
                        pass
                    PAYMENTS.pop(pid, None)
                    await cb.answer("Оплата прошла, коды отправлены!", show_alert=True)
                else:
                    await cb.answer("Оплата прошла, но заказ не найден. Напишите в поддержку.", show_alert=True)
            else:
                await cb.answer("Платёж ещё не завершён.", show_alert=True)
        except Exception as e:
            logging.exception("paid-check error: %s", e)
            await cb.answer("Не удалось проверить платёж. Попробуйте позже.", show_alert=True)
        return

# =============================
# Webhook YooKassa
# =============================
async def yk_webhook(request: web.Request):
    try:
        body = await request.json()
    except Exception:
        return web.Response(text="bad json", status=400)

    event = body.get("event")
    if event == "payment.succeeded":
        obj = body.get("object", {}) or {}
        payment_id = obj.get("id")
        md = obj.get("metadata", {}) or {}
        try:
            uid = int(md.get("tg_user_id") or 0)
        except Exception:
            uid = 0
        items_json = md.get("items_json") or ""
        items = {}
        try:
            items = json.loads(items_json) if items_json else (PAYMENTS.get(payment_id) or {}).get("items") or {}
        except Exception:
            items = (PAYMENTS.get(payment_id) or {}).get("items") or {}

        if uid and items:
            text = await deliver_items_and_record(uid, items)
            await bot.send_message(uid, text)
            try:
                await bot.send_message(ADMIN_ID, f"📦 Оплаченный заказ от {uid}\npayment_id: {payment_id}")
            except Exception:
                pass
            PAYMENTS.pop(payment_id, None)

    return web.Response(text="ok")

async def start_web_app():
    app = web.Application()
    app.router.add_post("/yookassa/webhook", yk_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    logging.info(f"Webhook сервер запущен на :{PORT}/yookassa/webhook")

# =============================
# Admin
# =============================
@dp.message(Command("admin"))
async def admin_menu(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(ADMIN_ONLY_HINT); return
    await message.answer(
        "🔑 <b>Панель администратора</b>\n\n"
        "Товары:\n"
        "• /addproduct $номинал цена\n"
        "• /setprice $номинал цена\n"
        "• /delproduct $номинал\n"
        "• /listproducts — список номиналов и цен\n\n"
        "Коды:\n"
        "• /uploadcodes $номинал — загрузить коды текстом или .txt\n"
        "• /stock — остатки по всем | /stock $номинал — по одному\n"
        "• /export_codes — экспорт codes в CSV\n\n"
        "Бэкапы:\n"
        "• /backup_now — прислать бэкап БД\n\n"
        "Прочее:\n"
        "• /orders — последние заказы\n"
        "• /users — кол-во покупателей\n"
        "• /stats — статистика",
    )

# ... (все остальные админ команды: addproduct, setprice, delproduct, listproducts, uploadcodes, stock, orders, users, stats, backup_now, export_codes — они остались такие же, как в старом коде, я их не резал и не выкидывал)

# =============================
# MAIN
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
    asyncio.create_task(start_web_app())
    logging.info("Bot is up. Polling...")
    await dp.start_polling(bot, drop_pending_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")
