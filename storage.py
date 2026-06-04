"""Единый доступ: Google Таблицы (если настроены) + SQLite для сессий."""

from __future__ import annotations

from typing import Any

from db import Database
from google_sheets import GoogleSheetsStore, SheetsConfig


class AppStorage:
    def __init__(self, db: Database, sheets: GoogleSheetsStore | None = None) -> None:
        self._db = db
        self._sheets = sheets

    @property
    def uses_sheets(self) -> bool:
        return self._sheets is not None

    # --- pending (always SQLite) ---
    async def init(self) -> None:
        await self._db.init()

    async def get_pending(self, user_id: int):
        return await self._db.get_pending(user_id)

    async def upsert_pending(self, user_id: int, **kwargs) -> None:
        await self._db.upsert_pending(user_id, **kwargs)

    async def set_pending_exact(self, user_id: int, **kwargs) -> None:
        await self._db.set_pending_exact(user_id, **kwargs)

    async def delete_pending(self, user_id: int) -> None:
        await self._db.delete_pending(user_id)

    async def get_registered_profile(self, user_id: int):
        return await self._db.get_registered_profile(user_id)

    # --- categories ---
    async def list_categories(self) -> list[dict[str, Any]]:
        if self._sheets:
            return self._sheets.list_categories()
        return await self._db.list_categories()

    async def list_enabled_categories(self) -> list[dict[str, Any]]:
        if self._sheets:
            return self._sheets.list_enabled_categories()
        return await self._db.list_enabled_categories()

    async def sync_categories_from_sheets_to_db(self) -> None:
        if not self._sheets:
            return
        cats = self._sheets.list_categories()
        if not cats:
            return
        await self._db.replace_categories([(c["name"], int(c["sort_order"])) for c in cats])

    async def add_category(self, name: str) -> None:
        await self._db.add_category(name)

    async def rename_category(self, cat_id: int, new_name: str) -> None:
        await self._db.rename_category(cat_id, new_name)

    async def toggle_category(self, cat_id: int) -> None:
        await self._db.toggle_category(cat_id)

    async def delete_category(self, cat_id: int) -> None:
        await self._db.delete_category(cat_id)

    # --- suppliers / customers ---
    async def find_suppliers(self, *, category: str, city: str | None, limit: int = 30) -> list[dict]:
        if self._sheets:
            return self._sheets.find_suppliers(category=category, city=city, limit=limit)
        return await self._db.find_suppliers(category=category, city=city, limit=limit)

    async def save_entry(self, *, user_id: int, role: str, phone: str, city: str | None, category: str) -> None:
        if self._sheets:
            if role == "supplier":
                self._sheets.append_supplier(
                    user_id=user_id, phone=phone, city=city, category=category
                )
            else:
                self._sheets.append_customer(
                    user_id=user_id, phone=phone, city=city, category=category
                )
        await self._db.save_entry(
            user_id=user_id, role=role, phone=phone, city=city, category=category
        )

    async def export_rows(self, role: str) -> list[dict]:
        return await self._db.export_rows(role)

    async def list_all_user_ids(self) -> list[int]:
        return await self._db.list_all_user_ids()

    async def find_matches(self) -> list[dict]:
        if self._sheets:
            return self._find_matches_sheets()
        return await self._db.find_matches()

    def _find_matches_sheets(self) -> list[dict]:
        assert self._sheets
        ws_c = self._sheets._ws("Заказчики")
        ws_s = self._sheets._ws("Поставщики")
        customers = ws_c.get_all_values()[1:]
        suppliers = ws_s.get_all_values()[1:]
        out: list[dict] = []
        for crow in customers:
            if len(crow) < 5:
                continue
            cat = (crow[4] if len(crow) > 4 else "").strip()
            if not cat:
                continue
            c_uid = crow[1] if len(crow) > 1 else ""
            c_phone = crow[2] if len(crow) > 2 else ""
            for srow in suppliers:
                if len(srow) < 6:
                    continue
                scat = (srow[5] if len(srow) > 5 else "").strip()
                if scat.casefold() != cat.casefold():
                    continue
                try:
                    c_uid_i = int(c_uid)
                except (TypeError, ValueError):
                    continue
                try:
                    s_uid_i = int(srow[9]) if len(srow) > 9 and str(srow[9]).strip() else 0
                except (TypeError, ValueError):
                    s_uid_i = 0
                if not s_uid_i:
                    continue
                out.append(
                    {
                        "norm_category": cat.casefold(),
                        "customer_category": cat,
                        "customer_user_id": c_uid_i,
                        "customer_phone": c_phone,
                        "supplier_user_id": s_uid_i,
                        "supplier_phone": (srow[7] if len(srow) > 7 else "") or (srow[8] if len(srow) > 8 else ""),
                    }
                )
        return out


def build_storage(db: Database, *, spreadsheet_id: str | None, credentials_path: str | None) -> AppStorage:
    sheets = None
    if spreadsheet_id and credentials_path:
        sheets = GoogleSheetsStore(
            SheetsConfig(spreadsheet_id=spreadsheet_id, credentials_path=credentials_path)
        )
    return AppStorage(db, sheets)
