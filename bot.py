#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BladeRunner Gift Card Bot (US only, 2/3/5/10/20) — aiogram 3 + SQLite + mock payments
- Catalog: $2, $3, $5, $10, $20
- SQLite stock with reservation + TTL
- Admin: /admin, /stock, /add <sku> <code>, CSV via caption /add_csv
- Payment: Mock button that marks as PAID and delivers the code
Deploy: run `python bot.py` (long polling) on Render/Railway worker
"""
import asyncio
import csv
import io
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Dict, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# -------- ENV --------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "BladeRunner_ru_bot")
DB_PATH = os.getenv("DB_PATH", "data.db")
RESERVE_TTL_MIN = int(os.getenv("RESERVE_TTL_MIN", "10"))  # minutes

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set. Add it in your hosting Environment Variables.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bladerunner-bot")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# -------- CATALOG (US only, 2/3/5/10/20) --------
# sku -> dict(title, price, currency)
# Цены по умолчанию равны номиналу. При необходимости меняйте здесь.
CATALOG: Dict[str, Dict] = {
    "us_2":  {"title": "Apple Gift Card (US) $2",  "price": 2.00, "currency": "USD"},
    "us_3":  {"title": "Apple Gift Card (US) $3",  "price": 3.00, "currency": "USD"},
    "us_5":  {"title": "Apple Gift Card (US) $5",  "price": 5.00, "currency": "USD"},
    "us_10": {"title": "Apple Gift Card (US) $10", "price": 10.00, "currency": "USD"},
    "us_20": {"title": "Apple Gift Card (US) $20", "price": 20.00, "currency": "USD"},
}

# -------- DB --------
DDL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS gift_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT NOT NULL,
    code TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'available', -- available | reserved | sold
    reserved_by INTEGER,
    reserved_at INTEGER,
    sold_to INTEGER,
    sold_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_gift_codes_sku_status ON gift_codes (sku, status);

CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    sku TEXT NOT NULL,
    price REAL NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL, -- pending | paid | cancelled | expired
    code_id INTEGER,
    created_at INTEGER NOT NULL,
    paid_at INTEGER,
    FOREIGN KEY(code_id) REFERENCES gift_codes(id)
);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders (user_id, created_at);
"""

@contextmanager
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with db() as conn:
        for stmt in DDL.strip().split(";\n"):
            s = (stmt + ";").strip()
            if s and s != ";":
                conn.execute(s)

def now_ts() -> int:
    return int(time.time())

def release_expired_reservations(conn: sqlite3.Connection, ttl_min: int):
    cutoff = now_ts() - ttl_min * 60
    cur = conn.execute(
        "UPDATE gift_codes SET status='available', reserved_by=NULL, reserved_at=NULL "
        "WHERE status='reserved' AND IFNULL(reserved_at,0) < ?", (cutoff,)
    )
    if cur.rowcount:
        log.info("Released %d expired reservations", cur.rowcount)

def stock_counts() -> Dict[str, int]:
    with db() as conn:
        release_expired_reservations(conn, RESERVE_TTL_MIN)
        rows = conn.execute(
            "SELECT sku, COUNT(*) as cnt FROM gift_codes WHERE status='available' GROUP BY sku"
        ).fetchall()
        res = {r["sku"]: r["cnt"] for r in rows}
        return res

def reserve_one_code(conn: sqlite3.Connection, sku: str, user_id: int) -> Optional[int]:
    """Reserve a single available code for user. Returns code_id or None."""
    conn.execute("BEGIN IMMEDIATE")
    row = conn.execute(
        "SELECT id FROM gift_codes WHERE sku=? AND status='available' ORDER BY id LIMIT 1",
        (sku,)
    ).fetchone()
    if not row:
        conn.execute("ROLLBACK")
        return None
    code_id = row["id"]
    conn.execute(
        "UPDATE gift_codes SET status='reserved', reserved_by=?, reserved_at=? WHERE id=? AND status='available'",
        (user_id, now_ts(), code_id)
    )
    conn.execute("COMMIT")
    return code_id

def create_order(user_id: int, sku: str) -> Optional[str]:
    if sku not in CATALOG:
        return None
    with db() as conn:
        release_expired_reservations(conn, RESERVE_TTL_MIN)
        code_id = reserve_one_code(conn, sku, user_id)
        if code_id is None:
            return None
        order_id = os.urandom(8).hex()
        item = CATALOG[sku]
        conn.execute(
            "INSERT INTO orders (id, user_id, sku, price, currency, status, code_id, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (order_id, user_id, sku, item["price"], item["currency"], "pending", code_id, now_ts())
        )
        return order_id

def load_order(order_id: str):
    with db() as conn:
        row = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        return row

def cancel_order(order_id: str, by_user: int) -> bool:
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order or order["status"] != "pending" or order["user_id"] != by_user:
            conn.execute("ROLLBACK")
            return False
        conn.execute("UPDATE gift_codes SET status='available', reserved_by=NULL, reserved_at=NULL WHERE id=?", (order["code_id"],))
        conn.execute("UPDATE orders SET status='cancelled' WHERE id=?", (order_id,))
        conn.execute("COMMIT")
        return True

def mark_paid_and_deliver(order_id: str, user_id: int) -> Optional[str]:
    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        order = conn.execute("SELECT * FROM orders WHERE id=?", (order_id,)).fetchone()
        if not order or order["status"] != "pending" or order["user_id"] != user_id:
            conn.execute("ROLLBACK")
            return None
        code_row = conn.execute("SELECT * FROM gift_codes WHERE id=?", (order["code_id"]),).fetchone()
        if not code_row or code_row["status"] not in ("reserved", "available"):
            conn.execute("ROLLBACK")
            return None
        conn.execute("UPDATE gift_codes SET status='sold', sold_to=?, sold_at=? WHERE id=?",
                     (user_id, now_ts(), order["code_id"]))
        conn.execute("UPDATE orders SET status='paid', paid_at=? WHERE id=?", (now_ts(), order_id))
        conn.execute("COMMIT")
        return code_row["code"]

# -------- Keyboards --------
def kb_catalog() -> InlineKeyboardMarkup:
    counts = stock_counts()
    b = InlineKeyboardBuilder()
    for sku in ("us_2", "us_3", "us_5", "us_10", "us_20"):
        item = CATALOG[sku]
        cnt = counts.get(sku, 0)
        text = f"{item['title']} — {item['price']} {item['currency']} ({cnt} шт)"
        b.button(text=text, callback_data=f"buy:{sku}")
    b.button(text="Обновить наличие ↻", callback_data="catalog:refresh")
    b.adjust(1)
    return b.as_markup()

def kb_payment(order_id: str) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✅ Оплатить (заглушка)", callback_data=f"pay:{order_id}")
    b.button(text="❌ Отменить", callback_data=f"cancel:{order_id}")
    b.adjust(1)
    return b.as_markup()

def kb_back_to_catalog() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="📦 Каталог", callback_data="catalog:open")
    b.adjust(1)
    return b.as_markup()

# -------- Handlers: User --------
@dp.message(CommandStart())
async def cmd_start(m: Message):
    await m.answer(
        f"Привет, <b>{m.from_user.full_name}</b>!\n"
        f"Это бот <b>BladeRunner</b> для продажи Apple Gift Cards (US).\n\n"
        f"Номиналы: $2 · $3 · $5 · $10 · $20.\n"
        f"Оплата пока через <i>заглушку</i> (для теста). Позже подключим платёжку.",
        reply_markup=kb_catalog()
    )

@dp.message(Command("catalog"))
async def cmd_catalog(m: Message):
    await m.answer("Каталог и наличие (US):", reply_markup=kb_catalog())

@dp.callback_query(F.data == "catalog:open")
@dp.callback_query(F.data == "catalog:refresh")
async def cb_catalog(cq: CallbackQuery):
    await cq.message.edit_text("Каталог и наличие (US):", reply_markup=kb_catalog())
    await cq.answer("Обновлено")

@dp.callback_query(F.data.startswith("buy:"))
async def cb_buy(cq: CallbackQuery):
    sku = cq.data.split(":", 1)[1]
    if sku not in CATALOG:
        await cq.answer("Товар не найден", show_alert=True)
        return
    order_id = create_order(cq.from_user.id, sku)
    if not order_id:
        await cq.answer("К сожалению, этого номинала нет в наличии.", show_alert=True)
        return
    item = CATALOG[sku]
    text = (
        f"<b>Заказ #{order_id}</b>\n"
        f"Товар: <b>{item['title']}</b>\n"
        f"Цена: <b>{item['price']} {item['currency']}</b>\n\n"
        f"Код зарезервирован на <b>{RESERVE_TTL_MIN} минут</b>. "
        f"Нажмите «Оплатить», чтобы получить код. "
        f"Если не успеете — бронь автоматически снимется."
    )
    if cq.message:
        await cq.message.edit_text(text, reply_markup=kb_payment(order_id))
    else:
        await cq.message.answer(text, reply_markup=kb_payment(order_id))
    await cq.answer()

@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(cq: CallbackQuery):
    order_id = cq.data.split(":", 1)[1]
    ok = cancel_order(order_id, cq.from_user.id)
    if ok:
        await cq.message.edit_text("Заказ отменён. Возвращаемся в каталог:", reply_markup=kb_catalog())
        await cq.answer("Отменено")
    else:
        await cq.answer("Не удалось отменить (возможно, заказ уже оплачен или истёк).", show_alert=True)

@dp.callback_query(F.data.startswith("pay:"))
async def cb_pay_mock(cq: CallbackQuery):
    order_id = cq.data.split(":", 1)[1]
    code = mark_paid_and_deliver(order_id, cq.from_user.id)
    if not code:
        await cq.answer("Оплата не прошла (или заказ уже обработан).", show_alert=True)
        return
    text = (
        f"✅ <b>Оплата подтверждена</b>\n"
        f"Ваш код:\n"
        f"<pre>{code}</pre>\n\n"
        f"<b>Как активировать (iPhone):</b>\n"
        f"App Store → профиль (иконка) → «Погасить подарочную карту или код» → введите код.\n\n"
        f"Если что-то пошло не так — напишите /support."
    )
    await cq.message.edit_text(text, reply_markup=kb_back_to_catalog())
    await cq.answer("Код выдан")

@dp.message(Command("support"))
async def cmd_support(m: Message):
    await m.answer("Поддержка: напишите ваш вопрос, номер заказа (если есть) и скрин. Админ свяжется.")

# -------- Handlers: Admin --------
def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID and ADMIN_ID != 0

ADMIN_HELP = (
    "<b>Админ-панель</b>\n"
    "Команды:\n"
    "• /stock — показать остатки по SKU\n"
    "• /add &lt;sku&gt; &lt;code&gt; — добавить один код (us_2|us_3|us_5|us_10|us_20)\n"
    "• Загрузить CSV (колонки: sku,code) как документ с подписью: /add_csv\n"
    "• /dump — выгрузить все доступные коды в CSV\n"
    "• /catalog — открыть каталог\n"
)

@dp.message(Command("admin"))
async def cmd_admin(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администратору.")
        return
    await m.answer(ADMIN_HELP)

@dp.message(Command("stock"))
async def cmd_stock(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администратору.")
        return
    counts = stock_counts()
    lines = ["<b>Остатки (US):</b>"]
    for sku, meta in CATALOG.items():
        lines.append(f"• {sku:<6} — {meta['title']}: <b>{counts.get(sku, 0)}</b> шт")
    await m.answer("\n".join(lines))

@dp.message(Command("add"))
async def cmd_add(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администратору.")
        return
    parts = m.text.split(maxsplit=2)
    if len(parts) < 3:
        await m.answer("Формат: /add <sku> <code>  (доступно: us_2, us_3, us_5, us_10, us_20)")
        return
    sku, code = parts[1].strip(), parts[2].strip()
    if sku not in CATALOG:
        await m.answer("Неизвестный SKU. Доступные: us_2, us_3, us_5, us_10, us_20")
        return
    try:
        with db() as conn:
            conn.execute("INSERT INTO gift_codes (sku, code, status) VALUES (?,?, 'available')", (sku, code))
        await m.answer(f"Добавлен код для {sku}: <code>{code}</code>")
    except sqlite3.IntegrityError:
        await m.answer("Такой код уже существует. Пропущен.")

@dp.message(F.document & (F.caption == "/add_csv"))
async def handle_csv_upload(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администратору.")
        return
    try:
        buf = io.BytesIO()
        await bot.download(m.document, destination=buf)  # aiogram v3 helper
        buf.seek(0)
        text = buf.read().decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(io.StringIO(text))
        added, skipped = 0, 0
        with db() as conn:
            for row in reader:
                sku = (row.get("sku") or "").strip()
                code = (row.get("code") or "").strip()
                if sku not in CATALOG or not code:
                    skipped += 1
                    continue
                try:
                    conn.execute("INSERT INTO gift_codes (sku, code, status) VALUES (?,?, 'available')", (sku, code))
                    added += 1
                except sqlite3.IntegrityError:
                    skipped += 1
        await m.answer(f"CSV обработан. Добавлено: <b>{added}</b>, пропущено: <b>{skipped}</b>.")
    except Exception as e:
        log.exception("CSV upload failed: %s", e)
        await m.answer("Не удалось обработать CSV. Убедитесь, что есть колонки: sku,code.")

@dp.message(Command("dump"))
async def cmd_dump(m: Message):
    if not is_admin(m.from_user.id):
        await m.answer("Команда доступна только администратору.")
        return
    with db() as conn:
        rows = conn.execute(
            "SELECT sku, code FROM gift_codes WHERE status='available' ORDER BY sku, id"
        ).fetchall()
    if not rows:
        await m.answer("Нет доступных кодов.")
        return
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["sku", "code"])
    for r in rows:
        w.writerow([r["sku"], r["code"]])
    out.seek(0)
    await m.answer_document(
        document=io.BytesIO(out.getvalue().encode("utf-8")),
        caption="Доступные коды (CSV)",
        filename="available_codes.csv"
    )

# -------- Startup --------
async def on_startup() -> None:
    init_db()
    log.info("DB ready at %s", DB_PATH)
    log.info("Bot username: @%s", BOT_USERNAME)

async def main():
    await on_startup()
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped.")
