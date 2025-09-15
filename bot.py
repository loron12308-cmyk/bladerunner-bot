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
BOT_TOKEN = os.getenv("BOT_TOKEN") or "YOUR_BOT_TOKEN"
ADMIN_ID = int(os.getenv("ADMIN_ID", "123456789"))

DB_PATH = os.getenv("DB_PATH", "store.db")

SITE_URL = os.getenv("SITE_URL", "https://t.me/your_channel")

# YooKassa
YK_SHOP_ID    = os.getenv("YK_SHOP_ID", "123456")
YK_SECRET_KEY = os.getenv("YK_SECRET_KEY", "your_secret_key")
YK_RETURN_URL = os.getenv("YK_RETURN_URL", "https://t.me/your_bot")
PORT          = int(os.getenv("PORT", "8080"))

BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")
BACKUP_EVERY_HOURS = int(os.getenv("BACKUP_EVERY_HOURS", "24"))

# =============================
# LOGGING & BOT
# =============================
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

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
        w.writerow(["id", "product_id", "code", "sold", "sold_at", "buyer_id"])
        for r in rows:
            w.writerow([r["id"], r["product_id"], r["code"], r["sold"], r["sold_at"], r["buyer_id"]])
    return csv_path


async def auto_backup_loop():
    if BACKUP_EVERY_HOURS <= 0:
        return
    while True:
        try:
            path = make_backup(DB_PATH)
            if ADMIN_ID:
                await bot.send_document(ADMIN_ID, FSInputFile(path), caption="Автобэкап БД ✅")
        except Exception as e:
            logging.exception("Ошибка автобэкапа: %s", e)
            if ADMIN_ID:
                await bot.send_message(ADMIN_ID, f"[backup] Ошибка автобэкапа: {e}")
        await asyncio.sleep(BACKUP_EVERY_HOURS * 3600)

  # =============================
# Helpers
# =============================
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID


def pay_kb(url: str, payment_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Оплатить через YooKassa", url=url)],
        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"paid:{payment_id}")]
    ])


async def deliver_items_and_record(uid: int, items: dict[str, dict]) -> str:
    """Пробегаемся по позициям, списываем коды, создаём заказ, возвращаем текст с кодами."""
    total = 0
    lines = []
    async with aiosqlite.connect(DB_PATH) as db:
        for pid_str, meta in items.items():
            pid = int(pid_str)
            name = meta["name"]
            qty  = int(meta["qty"])
            price = int(meta["price"])
            total += price * qty

            # забираем коды
            cur = await db.execute(
                "SELECT id, code FROM codes WHERE product_id = ? AND sold = 0 ORDER BY id LIMIT ?",
                (pid, qty)
            )
            codes_to_sell = await cur.fetchall()
            if len(codes_to_sell) < qty:
                # не хватает кодов
                await bot.send_message(
                    ADMIN_ID,
                    f"⚠️ Не хватает кодов для {name}. Нужно {qty}, есть {len(codes_to_sell)}. Покупатель: {uid}"
                )
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
    await message.answer(f"⭐ Отзывы и связь: {SITE_URL}")


@dp.message(F.text == "ℹ️ Помощь")
@dp.message(Command("help"))
async def help_cmd(message: types.Message):
    await message.answer(
        "ℹ️ Как это работает:\n"
        "• Откройте 📂 Каталог и выберите номинал ($2, $5, $10, $20, $25)\n"
        "• Товар добавится в 🛒 Корзину или купите прямо из карточки «Купить сейчас»\n"
        "• После оплаты коды придут сюда автоматически.\n\n"
        f"🌍 Отзывы: {SITE_URL}"
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
        await cb.answer()
        return

    if data == "back_to_catalog":
        kb = await build_catalog_kb()
        await cb.message.answer("📦 Выберите номинал карты:", reply_markup=kb)
        await cb.answer()
        return

    # карточка товара
    if data.startswith("buy:"):
        pid = int(data.split(":")[1])
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT name, price_rub FROM products WHERE id = ?", (pid,))
            row = await cur.fetchone()
        if not row:
            await cb.message.answer("❌ Товар не найден.")
            await cb.answer()
            return
        name, price = row
        text = product_card_text(name, price)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 Добавить в корзину", callback_data=f"add:{pid}")],
            [InlineKeyboardButton(text="⚡ Купить сейчас", callback_data=f"buy_now:{pid}")],
            [InlineKeyboardButton(text="⬅️ Назад к каталогу", callback_data="back_to_catalog")],
        ])
        await cb.message.answer(text, reply_markup=kb)
        await cb.answer()
        return

    # добавить в корзину
    if data.startswith("add:"):
        pid = int(data.split(":")[1])
        uid = cb.from_user.id
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT name, price_rub FROM products WHERE id = ?", (pid,))
            row = await cur.fetchone()
            if not row:
                await cb.message.answer("❌ Товар не найден.")
                await cb.answer()
                return
            name, price = row
            cur = await db.execute("SELECT qty FROM cart WHERE user_id = ? AND product_id = ?", (uid, pid))
            exist = await cur.fetchone()
            if exist:
                await db.execute("UPDATE cart SET qty = qty + 1 WHERE user_id = ? AND product_id = ?", (uid, pid))
            else:
                await db.execute("INSERT INTO cart (user_id, product_id, qty) VALUES (?, ?, 1)", (uid, pid))
            await db.commit()
        await cb.message.answer(f"✅ {name} — {price}₽ добавлен(а) в 🛒 Корзину.")
        await cb.answer()
        return

    # очистить корзину
    if data == "clear_cart":
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM cart WHERE user_id = ?", (cb.from_user.id,))
            await db.commit()
        await cb.message.answer("🗑️ Корзина очищена.")
        await cb.answer()
        return

    # купить сразу или checkout
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
                    await cb.message.answer("❌ Товар не найден.")
                    await cb.answer()
                    return
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
                    await cb.message.answer("❌ Корзина пуста.")
                    await cb.answer()
                    return
                for pid, name, price, qty in rows:
                    items_dict[str(pid)] = {"name": name, "qty": int(qty), "price": int(price)}
                    total += int(price) * int(qty)

        if Payment is None:
            await cb.message.answer("⚠️ Оплата временно недоступна. Свяжитесь с поддержкой.")
            await cb.answer()
            return

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
            await cb.answer()
            return

        confirm_url = payment.confirmation.confirmation_url
        PAYMENTS[payment.id] = {"user_id": uid, "items": items_dict}

        await cb.message.answer(
            f"✅ Заказ <b>#{order_id}</b>\n"
            f"Сумма к оплате: <b>{total}₽</b>\n\n"
            "Нажмите «Оплатить через ЮKassa». После оплаты бот автоматически отправит коды.",
            reply_markup=pay_kb(confirm_url, payment.id)
        )
        await cb.answer()
        return

      # ручная проверка оплаты (fallback, если автопроверка не сработала)
    if data.startswith("paid:"):
        pid = data.split(":")[1]
        if Payment is None:
            await cb.answer("Оплата отключена", show_alert=True)
            return
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
                        await bot.send_message(ADMIN_ID, f"📦 Оплаченный заказ от {info['user_id']}\n{datetime.utcnow().isoformat()}")
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
        await message.answer(ADMIN_ONLY_HINT)
        return
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


@dp.message(Command("addproduct"))
async def addproduct(message: types.Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split(maxsplit=2)
    if len(parts) != 3:
        await message.answer("❌ Используй: /addproduct $номинал цена")
        return
    name, price = parts[1], parts[2]
    try:
        price = int(price)
    except:
        await message.answer("❌ Цена должна быть числом."); return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO products (name, price_rub) VALUES (?, ?)", (name, price))
        await db.commit()
    await message.answer(f"✅ Номинал {name} добавлен с ценой {price}₽")


@dp.message(Command("setprice"))
async def setprice(message: types.Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split(maxsplit=2)
    if len(parts) != 3:
        await message.answer("❌ Используй: /setprice $номинал цена")
        return
    name, price = parts[1], parts[2]
    try:
        price = int(price)
    except:
        await message.answer("❌ Цена должна быть числом."); return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE products SET price_rub = ? WHERE name = ?", (price, name))
        await db.commit()
    await message.answer(f"✅ Цена для {name} установлена: {price}₽")


@dp.message(Command("delproduct"))
async def delproduct(message: types.Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("❌ Используй: /delproduct $номинал"); return
    name = parts[1]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM products WHERE name = ?", (name,))
        await db.commit()
    await message.answer(f"🗑️ Номинал {name} удалён")


@dp.message(Command("listproducts"))
async def listproducts(message: types.Message):
    if not is_admin(message.from_user.id): return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT name, price_rub FROM products ORDER BY id")
        rows = await cur.fetchall()
    if not rows:
        await message.answer("Пока нет номиналов."); return
    text = "📜 Номиналы и цены:\n" + "\n".join(f"• {n} — {p}₽" for n, p in rows)
    await message.answer(text)


@dp.message(Command("uploadcodes"))
async def uploadcodes(message: types.Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("❌ Используй: /uploadcodes $номинал\nЗатем пришли текст (по одному коду в строке) или .txt файл.")
        return
    name = parts[1]
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM products WHERE name = ?", (name,))
        row = await cur.fetchone()
    if not row:
        await message.answer("❌ Такого номинала нет. Добавьте через /addproduct."); return
    PENDING_UPLOAD[message.from_user.id] = row[0]
    await message.answer(f"📥 Жду коды для {name}. Пришли текст (по одному коду в строке) или .txt файл.")

@dp.message(F.document)
async def handle_doc(msg: types.Message):
    if not is_admin(msg.from_user.id): return
    if msg.from_user.id not in PENDING_UPLOAD: return
    pid = PENDING_UPLOAD[msg.from_user.id]
    file = await bot.get_file(msg.document.file_id)
    src = await bot.download_file(file.file_path)
    content = src.read().decode("utf-8", errors="ignore")
    count = await save_codes(pid, content)
    del PENDING_UPLOAD[msg.from_user.id]
    await msg.answer(f"✅ Загружено кодов: {count}")


@dp.message()
async def handle_text_after_upload(msg: types.Message):
    if is_admin(msg.from_user.id) and msg.from_user.id in PENDING_UPLOAD and msg.text and not msg.text.startswith("/"):
        pid = PENDING_UPLOAD[msg.from_user.id]
        count = await save_codes(pid, msg.text)
        del PENDING_UPLOAD[msg.from_user.id]
        await msg.answer(f"✅ Загружено кодов: {count}")
        return
    # остальные тексты пропускаем


async def save_codes(product_id: int, raw: str) -> int:
    lines = [l.strip() for l in raw.splitlines()]
    codes = [l for l in lines if l]
    saved = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for code in codes:
            try:
                await db.execute("INSERT OR IGNORE INTO codes (product_id, code) VALUES (?, ?)", (product_id, code))
                saved += 1
            except Exception:
                pass
        await db.commit()
    return saved


@dp.message(Command("stock"))
async def stock_cmd(message: types.Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    async with aiosqlite.connect(DB_PATH) as db:
        if len(parts) == 2:
            name = parts[1]
            cur = await db.execute("SELECT id FROM products WHERE name = ?", (name,))
            r = await cur.fetchone()
            if not r: await message.answer("❌ Номинал не найден."); return
            pid = r[0]
            cur = await db.execute("SELECT COUNT(*) FROM codes WHERE product_id = ? AND sold = 0", (pid,))
            free = (await cur.fetchone())[0]
            await message.answer(f"📦 Остаток {name}: {free} шт")
        else:
            cur = await db.execute("""
                SELECT p.name, COUNT(c.id) as free_cnt
                FROM products p
                LEFT JOIN codes c ON c.product_id = p.id AND c.sold = 0
                GROUP BY p.id ORDER BY p.id
            """)
            rows = await cur.fetchall()
            if not rows:
                await message.answer("Номиналов нет."); return
            text = "📦 Остатки по номиналам:\n" + "\n".join(f"• {n}: {cnt} шт" for n, cnt in rows)
            await message.answer(text)


@dp.message(Command("orders"))
async def orders_cmd(message: types.Message):
    if not is_admin(message.from_user.id): return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, user_id, total_rub, created_at FROM orders ORDER BY id DESC LIMIT 10")
        rows = await cur.fetchall()
    if not rows:
        await message.answer("❌ Заказов нет."); return
    text = "📜 Последние заказы:\n" + "\n".join(
        f"#{oid} — user {uid} — {total}₽ — {ts}" for oid, uid, total, ts in rows
    )
    await message.answer(text)


@dp.message(Command("users"))
async def users_cmd(message: types.Message):
    if not is_admin(message.from_user.id): return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(DISTINCT user_id) FROM orders")
        cnt = (await cur.fetchone())[0]
    await message.answer(f"👥 Покупателей (с заказами): {cnt}")


@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    if not is_admin(message.from_user.id): return
    async with aiosqlite.connect(DB_PATH) as db:
        cur1 = await db.execute("SELECT COUNT(*) FROM orders")
        orders_cnt = (await cur1.fetchone())[0]
        cur2 = await db.execute("SELECT COALESCE(SUM(total_rub),0) FROM orders")
        revenue = (await cur2.fetchone())[0]
    await message.answer(f"📊 Статистика:\n📦 Заказов: {orders_cnt}\n💰 Выручка: {revenue}₽")


@dp.message(Command("backup_now"))
async def backup_now_cmd(message: types.Message):
    if not is_admin(message.from_user.id): return
    try:
        path = make_backup(DB_PATH)
        await message.answer_document(FSInputFile(path), caption="Бэкап готов ✅")
    except Exception as e:
        await message.answer(f"Ошибка бэкапа: {e}")


@dp.message(Command("export_codes"))
async def export_codes_cmd(message: types.Message):
    if not is_admin(message.from_user.id): return
    try:
        path = await export_codes_csv(DB_PATH)
        await message.answer_document(FSInputFile(path), caption="Экспорт codes.csv ✅")
    except Exception as e:
        await message.answer(f"Ошибка экспорта: {e}")

  # =============================
# MAIN
# =============================
async def main():
    await init_db()
    # меню команд только пользовательские
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Запуск"),
        types.BotCommand(command="catalog", description="Каталог"),
        types.BotCommand(command="cart", description="Корзина"),
        types.BotCommand(command="reviews", description="Отзывы"),
        types.BotCommand(command="help", description="Помощь"),
    ])
    asyncio.create_task(start_web_app())
    asyncio.create_task(auto_backup_loop())
    logging.info("Bot is up. Polling...")
    await dp.start_polling(bot, drop_pending_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")
