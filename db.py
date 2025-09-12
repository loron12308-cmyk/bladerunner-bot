import aiosqlite
import os

DB_PATH = "database.db"

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                is_sold INTEGER DEFAULT 0
            );
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                card_code TEXT,
                nominal TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        await db.commit()

async def get_random_card():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, code FROM cards WHERE is_sold = 0 ORDER BY RANDOM() LIMIT 1") as cursor:
            row = await cursor.fetchone()
            return {"id": row[0], "code": row[1]} if row else None

async def mark_card_as_sold(card_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE cards SET is_sold = 1 WHERE id = ?", (card_id,))
        await db.commit()

async def log_order(user_id: int, card_code: str, nominal: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO orders (user_id, card_code, nominal) VALUES (?, ?, ?)",
            (user_id, card_code, nominal)
        )
        await db.commit()