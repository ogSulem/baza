"""Google Таблицы — данные; SQLite — только сессии и админ-категории."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from db import Database
from google_sheets import GoogleSheetsStore, SheetsConfig

log = logging.getLogger(__name__)


class AppStorage:
    def __init__(
        self,
        db: Database,
        *,
        spreadsheet_id: str,
        credentials_path: str,
    ) -> None:
        self._db = db
        self._sheets_cfg = SheetsConfig(
            spreadsheet_id=spreadsheet_id,
            credentials_path=credentials_path,
        )
        self._sheets: GoogleSheetsStore | None = None

    @property
    def uses_sheets(self) -> bool:
        return self._sheets is not None

    async def _run(self, fn, *args, **kwargs):
        assert self._sheets is not None
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def init(self) -> None:
        await self._db.init()
        self._sheets = await asyncio.to_thread(GoogleSheetsStore, self._sheets_cfg)
        info = await self._run(self._sheets.verify_connection)
        log.info("Google Sheets connected: %s", info)

    async def get_pending(self, user_id: int):
        return await self._db.get_pending(user_id)

    async def upsert_pending(self, user_id: int, **kwargs) -> None:
        await self._db.upsert_pending(user_id, **kwargs)

    async def set_pending_exact(self, user_id: int, **kwargs) -> None:
        await self._db.set_pending_exact(user_id, **kwargs)

    async def delete_pending(self, user_id: int) -> None:
        await self._db.delete_pending(user_id)

    async def get_registered_profile(self, user_id: int):
        pending = await self._db.get_pending(user_id)
        if pending and pending.phone:
            return (pending.phone, pending.city)
        return await self._run(self._sheets.get_user_profile, user_id)

    async def list_categories(self) -> list[dict[str, Any]]:
        return await self._db.list_categories()

    async def list_enabled_categories(self) -> list[dict[str, Any]]:
        return await self._db.list_enabled_categories()

    async def add_category(self, name: str) -> None:
        await self._db.add_category(name)

    async def rename_category(self, cat_id: int, new_name: str) -> None:
        await self._db.rename_category(cat_id, new_name)

    async def toggle_category(self, cat_id: int) -> None:
        await self._db.toggle_category(cat_id)

    async def delete_category(self, cat_id: int) -> None:
        await self._db.delete_category(cat_id)

    async def find_suppliers(
        self,
        *,
        query: str | None = None,
        category: str | None = None,
        city: str | None,
        limit: int = 30,
    ) -> list[dict]:
        q = (query or category or "").strip()
        return await self._run(self._sheets.find_suppliers, query=q, city=city, limit=limit)

    async def save_entry(
        self,
        *,
        user_id: int,
        role: str,
        phone: str,
        city: str | None,
        category: str,
        name: str | None = None,
    ) -> None:
        if role == "supplier":
            await self._run(
                self._sheets.upsert_supplier,
                user_id=user_id,
                phone=phone,
                city=city,
                category=category,
                name=name,
            )
        else:
            await self._run(
                self._sheets.upsert_customer,
                user_id=user_id,
                phone=phone,
                city=city,
                category=category,
                name=name,
            )

    async def export_rows(self, role: str) -> list[dict]:
        return await self._run(self._sheets.export_rows, role)

    async def list_all_user_ids(self) -> list[int]:
        return await self._run(self._sheets.list_telegram_user_ids)

    async def find_matches(self) -> list[dict]:
        return await self._run(self._sheets.find_matches)


def build_storage(db: Database, *, spreadsheet_id: str, credentials_path: str) -> AppStorage:
    return AppStorage(db, spreadsheet_id=spreadsheet_id, credentials_path=credentials_path)
