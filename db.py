from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import aiosqlite


@dataclass
class PendingUser:
    user_id: int
    phone: str | None
    city: str | None
    role: str | None  # supplier|customer
    state: str | None  # admin_add|admin_rename|...
    payload: str | None
    bot_message_id: int | None
    menu_message_id: int | None


DEFAULT_CATEGORIES: list[tuple[int, str]] = [
    (1, "Пилорама"),
    (2, "Доски"),
    (3, "Гвозди"),
    (4, "Другое"),
]


class Database:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path

    async def connect(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self._db_path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        return conn

    async def init(self) -> None:
        db = await self.connect()
        try:
            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS pending_users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    city TEXT,
                    role TEXT,
                    state TEXT,
                    payload TEXT,
                    bot_message_id INTEGER,
                    menu_message_id INTEGER,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    sort_order INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS suppliers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    phone TEXT NOT NULL,
                    city TEXT,
                    category TEXT NOT NULL,
                    name TEXT,
                    source TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    phone TEXT NOT NULL,
                    city TEXT,
                    category TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

            # Lightweight migrations for older DBs
            cur = await db.execute("PRAGMA table_info(pending_users)")
            cols = {row["name"] for row in await cur.fetchall()}
            if "city" not in cols:
                await db.execute("ALTER TABLE pending_users ADD COLUMN city TEXT")
            if "menu_message_id" not in cols:
                await db.execute("ALTER TABLE pending_users ADD COLUMN menu_message_id INTEGER")
            if "state" not in cols:
                await db.execute("ALTER TABLE pending_users ADD COLUMN state TEXT")
            if "payload" not in cols:
                await db.execute("ALTER TABLE pending_users ADD COLUMN payload TEXT")

            cur = await db.execute("PRAGMA table_info(suppliers)")
            cols = {row["name"] for row in await cur.fetchall()}
            if "city" not in cols:
                await db.execute("ALTER TABLE suppliers ADD COLUMN city TEXT")
            if "name" not in cols:
                await db.execute("ALTER TABLE suppliers ADD COLUMN name TEXT")
            if "source" not in cols:
                await db.execute("ALTER TABLE suppliers ADD COLUMN source TEXT")

            cur = await db.execute("PRAGMA table_info(customers)")
            cols = {row["name"] for row in await cur.fetchall()}
            if "city" not in cols:
                await db.execute("ALTER TABLE customers ADD COLUMN city TEXT")

            cur = await db.execute("SELECT COUNT(*) AS c FROM categories")
            row = await cur.fetchone()
            if int(row["c"]) == 0:
                await db.executemany(
                    "INSERT INTO categories(name, sort_order, enabled) VALUES(?, ?, 1)",
                    [(name, sort_order) for sort_order, name in DEFAULT_CATEGORIES],
                )

            await db.commit()
        finally:
            await db.close()

    async def set_pending_exact(
        self,
        user_id: int,
        *,
        phone: str | None,
        city: str | None,
        role: str | None,
        state: str | None,
        payload: str | None,
        bot_message_id: int | None,
        menu_message_id: int | None,
    ) -> None:
        now = dt.datetime.utcnow().isoformat()
        db = await self.connect()
        try:
            await db.execute(
                """
                INSERT INTO pending_users(user_id, phone, city, role, state, payload, bot_message_id, menu_message_id, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    phone=excluded.phone,
                    city=excluded.city,
                    role=excluded.role,
                    state=excluded.state,
                    payload=excluded.payload,
                    bot_message_id=excluded.bot_message_id,
                    menu_message_id=excluded.menu_message_id,
                    updated_at=excluded.updated_at;
                """,
                (user_id, phone, city, role, state, payload, bot_message_id, menu_message_id, now),
            )
            await db.commit()
        finally:
            await db.close()

    async def list_all_user_ids(self) -> list[int]:
        db = await self.connect()
        try:
            cur = await db.execute(
                """
                SELECT user_id FROM suppliers
                UNION
                SELECT user_id FROM customers
                """
            )
            rows = await cur.fetchall()
            return [int(r["user_id"]) for r in rows]
        finally:
            await db.close()

    async def find_matches(self) -> list[dict]:
        """Return pairs supplier<->customer with same category (case-insensitive)."""
        db = await self.connect()
        try:
            cur = await db.execute(
                """
                SELECT
                    LOWER(c.category) AS norm_category,
                    c.category AS customer_category,
                    c.user_id AS customer_user_id,
                    c.phone AS customer_phone,
                    s.user_id AS supplier_user_id,
                    s.phone AS supplier_phone
                FROM customers c
                JOIN suppliers s
                  ON LOWER(c.category) = LOWER(s.category)
                ORDER BY norm_category ASC, c.id DESC, s.id DESC
                """
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            await db.close()

    async def upsert_pending(
        self,
        user_id: int,
        *,
        phone: str | None = None,
        city: str | None = None,
        role: str | None = None,
        state: str | None = None,
        payload: str | None = None,
        bot_message_id: int | None = None,
        menu_message_id: int | None = None,
    ) -> None:
        now = dt.datetime.utcnow().isoformat()
        db = await self.connect()
        try:
            await db.execute(
                """
                INSERT INTO pending_users(user_id, phone, city, role, state, payload, bot_message_id, menu_message_id, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    phone=COALESCE(excluded.phone, pending_users.phone),
                    city=COALESCE(excluded.city, pending_users.city),
                    role=COALESCE(excluded.role, pending_users.role),
                    state=COALESCE(excluded.state, pending_users.state),
                    payload=COALESCE(excluded.payload, pending_users.payload),
                    bot_message_id=COALESCE(excluded.bot_message_id, pending_users.bot_message_id),
                    menu_message_id=COALESCE(excluded.menu_message_id, pending_users.menu_message_id),
                    updated_at=excluded.updated_at;
                """,
                (user_id, phone, city, role, state, payload, bot_message_id, menu_message_id, now),
            )
            await db.commit()
        finally:
            await db.close()

    async def get_pending(self, user_id: int) -> PendingUser | None:
        db = await self.connect()
        try:
            cur = await db.execute(
                "SELECT user_id, phone, city, role, state, payload, bot_message_id, menu_message_id FROM pending_users WHERE user_id=?",
                (user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            return PendingUser(
                user_id=int(row["user_id"]),
                phone=row["phone"],
                city=row["city"],
                role=row["role"],
                state=row["state"],
                payload=row["payload"],
                bot_message_id=row["bot_message_id"],
                menu_message_id=row["menu_message_id"],
            )
        finally:
            await db.close()

    async def delete_pending(self, user_id: int) -> None:
        db = await self.connect()
        try:
            await db.execute("DELETE FROM pending_users WHERE user_id=?", (user_id,))
            await db.commit()
        finally:
            await db.close()

    async def list_categories(self) -> list[dict]:
        db = await self.connect()
        try:
            cur = await db.execute(
                "SELECT id, name, sort_order, enabled FROM categories ORDER BY sort_order ASC, id ASC"
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            await db.close()

    async def list_enabled_categories(self) -> list[dict]:
        db = await self.connect()
        try:
            cur = await db.execute(
                "SELECT id, name, sort_order FROM categories WHERE enabled=1 ORDER BY sort_order ASC, id ASC"
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            await db.close()

    async def get_category(self, category_id: int) -> dict | None:
        db = await self.connect()
        try:
            cur = await db.execute(
                "SELECT id, name, sort_order, enabled FROM categories WHERE id=?",
                (category_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None
        finally:
            await db.close()

    async def add_category(self, name: str) -> None:
        db = await self.connect()
        try:
            cur = await db.execute("SELECT COALESCE(MAX(sort_order), 0)+1 AS nxt FROM categories")
            row = await cur.fetchone()
            nxt = int(row["nxt"])
            await db.execute(
                "INSERT INTO categories(name, sort_order, enabled) VALUES(?, ?, 1)",
                (name, nxt),
            )
            await db.commit()
        finally:
            await db.close()

    async def rename_category(self, category_id: int, new_name: str) -> None:
        db = await self.connect()
        try:
            await db.execute(
                "UPDATE categories SET name=? WHERE id=?",
                (new_name, category_id),
            )
            await db.commit()
        finally:
            await db.close()

    async def toggle_category(self, category_id: int) -> None:
        db = await self.connect()
        try:
            await db.execute(
                "UPDATE categories SET enabled=CASE WHEN enabled=1 THEN 0 ELSE 1 END WHERE id=?",
                (category_id,),
            )
            await db.commit()
        finally:
            await db.close()

    async def delete_category(self, category_id: int) -> None:
        db = await self.connect()
        try:
            await db.execute("DELETE FROM categories WHERE id=?", (category_id,))
            await db.commit()
        finally:
            await db.close()

    async def move_category(self, category_id: int, direction: str) -> bool:
        if direction not in {"up", "down"}:
            raise ValueError("direction must be 'up' or 'down'")

        db = await self.connect()
        try:
            cur = await db.execute(
                "SELECT id, sort_order FROM categories WHERE id=?",
                (category_id,),
            )
            current = await cur.fetchone()
            if not current:
                return False

            current_sort = int(current["sort_order"])

            if direction == "up":
                cur = await db.execute(
                    """
                    SELECT id, sort_order
                    FROM categories
                    WHERE sort_order < ?
                    ORDER BY sort_order DESC, id DESC
                    LIMIT 1
                    """,
                    (current_sort,),
                )
            else:
                cur = await db.execute(
                    """
                    SELECT id, sort_order
                    FROM categories
                    WHERE sort_order > ?
                    ORDER BY sort_order ASC, id ASC
                    LIMIT 1
                    """,
                    (current_sort,),
                )

            neighbor = await cur.fetchone()
            if not neighbor:
                return False

            neighbor_id = int(neighbor["id"])
            neighbor_sort = int(neighbor["sort_order"])

            await db.execute("BEGIN")
            await db.execute(
                "UPDATE categories SET sort_order=? WHERE id=?",
                (neighbor_sort, category_id),
            )
            await db.execute(
                "UPDATE categories SET sort_order=? WHERE id=?",
                (current_sort, neighbor_id),
            )
            await db.commit()
            return True
        finally:
            await db.close()

    async def save_entry(self, *, user_id: int, role: str, phone: str, city: str | None, category: str) -> None:
        now = dt.datetime.utcnow().isoformat()
        table = "suppliers" if role == "supplier" else "customers"
        db = await self.connect()
        try:
            await db.execute(
                f"INSERT INTO {table}(user_id, phone, city, category, created_at) VALUES(?, ?, ?, ?, ?)",
                (user_id, phone, city, category, now),
            )
            await db.commit()
        finally:
            await db.close()

    async def add_supplier(
        self,
        *,
        user_id: int,
        phone: str,
        city: str | None,
        category: str,
        name: str | None,
        source: str | None,
    ) -> None:
        now = dt.datetime.utcnow().isoformat()
        db = await self.connect()
        try:
            await db.execute(
                """
                INSERT INTO suppliers(user_id, phone, city, category, name, source, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, phone, city, category, name, source, now),
            )
            await db.commit()
        finally:
            await db.close()

    async def export_rows(self, role: str) -> list[dict]:
        table = "suppliers" if role == "supplier" else "customers"
        db = await self.connect()
        try:
            cur = await db.execute(
                f"SELECT id, user_id, phone, city, category, created_at FROM {table} ORDER BY id DESC"
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            await db.close()

    async def find_suppliers(self, *, category: str, city: str | None, limit: int = 30) -> list[dict]:
        category = (category or "").strip()
        if not category:
            return []

        db = await self.connect()
        try:
            if city:
                cur = await db.execute(
                    """
                    SELECT s.user_id, s.phone, s.city, s.category, s.name, s.source, s.created_at
                    FROM suppliers s
                    JOIN (
                        SELECT user_id, MAX(id) AS max_id
                        FROM suppliers
                        WHERE LOWER(category) = LOWER(?) AND city = ?
                        GROUP BY user_id
                    ) last
                      ON last.user_id = s.user_id AND last.max_id = s.id
                    ORDER BY s.id DESC
                    LIMIT ?
                    """,
                    (category, city, int(limit)),
                )
                rows = await cur.fetchall()
                return [dict(r) for r in rows]

            cur = await db.execute(
                """
                SELECT s.user_id, s.phone, s.city, s.category, s.name, s.source, s.created_at
                FROM suppliers s
                JOIN (
                    SELECT user_id, MAX(id) AS max_id
                    FROM suppliers
                    WHERE LOWER(category) = LOWER(?)
                    GROUP BY user_id
                ) last
                  ON last.user_id = s.user_id AND last.max_id = s.id
                ORDER BY s.id DESC
                LIMIT ?
                """,
                (category, int(limit)),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            await db.close()

    async def is_registered(self, user_id: int) -> bool:
        db = await self.connect()
        try:
            cur = await db.execute("SELECT 1 FROM suppliers WHERE user_id=? LIMIT 1", (user_id,))
            if await cur.fetchone():
                return True
            cur = await db.execute("SELECT 1 FROM customers WHERE user_id=? LIMIT 1", (user_id,))
            return (await cur.fetchone()) is not None
        finally:
            await db.close()

    async def get_registered_profile(self, user_id: int) -> tuple[str | None, str | None]:
        db = await self.connect()
        try:
            cur = await db.execute(
                "SELECT phone, city FROM suppliers WHERE user_id=? ORDER BY id DESC LIMIT 1",
                (user_id,),
            )
            row = await cur.fetchone()
            if row and row["phone"]:
                return (str(row["phone"]), row["city"])
            cur = await db.execute(
                "SELECT phone, city FROM customers WHERE user_id=? ORDER BY id DESC LIMIT 1",
                (user_id,),
            )
            row = await cur.fetchone()
            if row and row["phone"]:
                return (str(row["phone"]), row["city"])
            return (None, None)
        finally:
            await db.close()

    async def get_registered_phone(self, user_id: int) -> str | None:
        phone, _city = await self.get_registered_profile(user_id)
        return phone
