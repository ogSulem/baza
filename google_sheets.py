"""Google Sheets: поставщики, заказчики, категории (2+ листа в одной таблице)."""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover
    gspread = None
    Credentials = None

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_SUPPLIERS = "Поставщики"
SHEET_CUSTOMERS = "Заказчики"
SHEET_CATEGORIES = "Категории"

SUPPLIER_HEADERS = [
    "id",
    "город",
    "регион",
    "раздел",
    "материал",
    "категория",
    "имя",
    "телефон",
    "ссылка",
    "telegram_user_id",
    "источник",
    "обновлено",
]

CUSTOMER_HEADERS = [
    "id",
    "telegram_user_id",
    "телефон",
    "город",
    "категория",
    "имя",
    "обновлено",
]

CATEGORY_HEADERS = ["sort_order", "категория", "включена"]


@dataclass(frozen=True)
class SheetsConfig:
    spreadsheet_id: str
    credentials_path: str


class GoogleSheetsStore:
    def __init__(self, cfg: SheetsConfig) -> None:
        if gspread is None or Credentials is None:
            raise RuntimeError("Install: pip install gspread google-auth")
        creds = Credentials.from_service_account_file(cfg.credentials_path, scopes=SCOPES)
        self._gc = gspread.authorize(creds)
        self._spreadsheet = self._gc.open_by_key(cfg.spreadsheet_id)
        self._ensure_sheets()

    def _ensure_sheets(self) -> None:
        titles = {ws.title for ws in self._spreadsheet.worksheets()}
        if SHEET_SUPPLIERS not in titles:
            self._spreadsheet.add_worksheet(SHEET_SUPPLIERS, rows=2000, cols=12)
        if SHEET_CUSTOMERS not in titles:
            self._spreadsheet.add_worksheet(SHEET_CUSTOMERS, rows=500, cols=8)
        if SHEET_CATEGORIES not in titles:
            self._spreadsheet.add_worksheet(SHEET_CATEGORIES, rows=100, cols=3)

        for title, headers in (
            (SHEET_SUPPLIERS, SUPPLIER_HEADERS),
            (SHEET_CUSTOMERS, CUSTOMER_HEADERS),
            (SHEET_CATEGORIES, CATEGORY_HEADERS),
        ):
            ws = self._spreadsheet.worksheet(title)
            first = ws.row_values(1)
            if not first:
                ws.append_row(headers, value_input_option="RAW")

    def _ws(self, name: str):
        return self._spreadsheet.worksheet(name)

    @staticmethod
    def _row_dict(headers: list[str], row: list[str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for i, h in enumerate(headers):
            out[h] = (row[i] if i < len(row) else "").strip()
        return out

    def list_categories(self) -> list[dict[str, Any]]:
        ws = self._ws(SHEET_CATEGORIES)
        rows = ws.get_all_values()
        if not rows:
            return []
        headers = [h.strip().casefold() for h in rows[0]]
        idx_name = headers.index("категория") if "категория" in headers else 1
        idx_en = headers.index("включена") if "включена" in headers else 2
        idx_sort = headers.index("sort_order") if "sort_order" in headers else 0
        out: list[dict[str, Any]] = []
        for n, row in enumerate(rows[1:], start=1):
            if not any(cell.strip() for cell in row):
                continue
            name = row[idx_name].strip() if idx_name < len(row) else ""
            if not name:
                continue
            enabled_raw = row[idx_en].strip() if idx_en < len(row) else "1"
            enabled = enabled_raw not in {"0", "нет", "false", "выкл", "off"}
            try:
                sort_order = int(row[idx_sort]) if idx_sort < len(row) and row[idx_sort].strip() else n
            except ValueError:
                sort_order = n
            out.append({"id": n, "name": name, "enabled": 1 if enabled else 0, "sort_order": sort_order})
        out.sort(key=lambda x: (int(x["sort_order"]), x["name"].casefold()))
        return out

    def list_enabled_categories(self) -> list[dict[str, Any]]:
        return [c for c in self.list_categories() if int(c["enabled"]) == 1]

    def find_suppliers(self, *, category: str, city: str | None, limit: int = 30) -> list[dict]:
        ws = self._ws(SHEET_SUPPLIERS)
        rows = ws.get_all_values()
        if len(rows) < 2:
            return []

        headers = rows[0]
        cat_key = category.strip().casefold()
        seen: set[str] = set()
        matched: list[dict] = []
        other: list[dict] = []

        for row in reversed(rows[1:]):
            d = self._row_dict(headers, row)
            row_cat = (d.get("категория") or "").strip()
            if row_cat.casefold() != cat_key:
                continue
            phone = (d.get("телефон") or "").strip()
            link = (d.get("ссылка") or "").strip()
            if not phone and link:
                phone = link
            key = f"{phone}|{link}|{(d.get('имя') or '').strip()}"
            if key in seen:
                continue
            seen.add(key)

            item = {
                "user_id": (d.get("telegram_user_id") or "").strip(),
                "phone": phone,
                "city": (d.get("город") or "").strip(),
                "category": row_cat,
                "name": (d.get("имя") or "").strip(),
                "source": link or None,
                "created_at": (d.get("обновлено") or "").strip(),
            }
            row_city = (item["city"] or "").strip()
            if city and row_city == city:
                matched.append(item)
            else:
                other.append(item)

        out = matched if matched else other
        return out[:limit]

    def append_supplier(
        self,
        *,
        user_id: int,
        phone: str,
        city: str | None,
        category: str,
        name: str | None = None,
        source: str | None = None,
        region: str = "",
        section: str = "",
        material: str = "",
    ) -> None:
        now = dt.datetime.utcnow().isoformat(timespec="seconds")
        link = source or ""
        tel = phone
        if tel.startswith("http"):
            link, tel = tel, ""
        ws = self._ws(SHEET_SUPPLIERS)
        next_id = max(0, len(ws.get_all_values()) - 1) + 1
        ws.append_row(
            [
                next_id,
                city or "",
                region,
                section,
                material,
                category,
                name or "",
                tel,
                link,
                str(user_id),
                "telegram",
                now,
            ],
            value_input_option="RAW",
        )

    def append_customer(
        self,
        *,
        user_id: int,
        phone: str,
        city: str | None,
        category: str,
        name: str | None = None,
    ) -> None:
        now = dt.datetime.utcnow().isoformat(timespec="seconds")
        ws = self._ws(SHEET_CUSTOMERS)
        next_id = max(0, len(ws.get_all_values()) - 1) + 1
        ws.append_row(
            [next_id, str(user_id), phone, city or "", category, name or "", now],
            value_input_option="RAW",
        )

    def replace_categories(self, names: list[str]) -> None:
        ws = self._ws(SHEET_CATEGORIES)
        ws.clear()
        ws.append_row(CATEGORY_HEADERS, value_input_option="RAW")
        for i, name in enumerate(names, start=1):
            ws.append_row([i, name, 1], value_input_option="RAW")

    def upload_suppliers_bulk(self, rows: list[list]) -> None:
        """rows without header; clears suppliers sheet and rewrites."""
        ws = self._ws(SHEET_SUPPLIERS)
        ws.clear()
        ws.append_row(SUPPLIER_HEADERS, value_input_option="RAW")
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
