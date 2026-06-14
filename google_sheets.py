"""Google Таблицы — единый источник данных поставщиков и заказчиков."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from contact_utils import normalize_phone
from search_match import supply_matches

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:  # pragma: no cover
    gspread = None
    Credentials = None

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_SUPPLIERS = "Поставщики"
SHEET_CUSTOMERS = "Заказчики"

SUPPLIER_HEADERS = ["id", "город", "что_поставляет", "телефон", "имя", "telegram_user_id", "обновлено"]
CUSTOMER_HEADERS = ["id", "город", "что_нужно", "телефон", "имя", "telegram_user_id", "обновлено"]


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

    def verify_connection(self) -> str:
        title = self._spreadsheet.title
        ws = self._ws(SHEET_SUPPLIERS)
        count = max(0, len(ws.get_all_values()) - 1)
        return f"{title!r}, лист «{SHEET_SUPPLIERS}»: {count} строк"

    def _ensure_sheets(self) -> None:
        titles = {ws.title for ws in self._spreadsheet.worksheets()}
        if SHEET_SUPPLIERS not in titles:
            self._spreadsheet.add_worksheet(SHEET_SUPPLIERS, rows=3000, cols=8)
        if SHEET_CUSTOMERS not in titles:
            self._spreadsheet.add_worksheet(SHEET_CUSTOMERS, rows=500, cols=8)

        for title, headers in (
            (SHEET_SUPPLIERS, SUPPLIER_HEADERS),
            (SHEET_CUSTOMERS, CUSTOMER_HEADERS),
        ):
            ws = self._spreadsheet.worksheet(title)
            if not ws.row_values(1):
                ws.append_row(headers, value_input_option="RAW")

    def _ws(self, name: str):
        return self._spreadsheet.worksheet(name)

    @staticmethod
    def _indexes(headers: list[str]) -> dict[str, int | None]:
        norm = [h.strip().casefold() for h in headers]

        def idx(*names: str) -> int | None:
            for n in names:
                k = n.casefold()
                if k in norm:
                    return norm.index(k)
            return None

        return {
            "id": idx("id"),
            "city": idx("город"),
            "supply": idx("что_поставляет", "что_нужно", "категория"),
            "phone": idx("телефон", "ссылка"),
            "name": idx("имя"),
            "uid": idx("telegram_user_id"),
            "updated": idx("обновлено"),
        }

    @staticmethod
    def _cell(row: list[str], i: int | None) -> str:
        if i is None or i >= len(row):
            return ""
        return (row[i] or "").strip()

    @staticmethod
    def _normalize_telegram_phone(phone: str) -> str:
        if phone.startswith("http"):
            return phone
        return normalize_phone(phone) or phone

    @staticmethod
    def _now_iso() -> str:
        return dt.datetime.utcnow().isoformat(timespec="seconds")

    def _next_id(self, rows: list[list[str]]) -> int:
        max_id = 0
        for row in rows[1:]:
            try:
                max_id = max(max_id, int((row[0] if row else "0") or 0))
            except (TypeError, ValueError):
                continue
        return max_id + 1

    def _find_row_by_user_id(self, rows: list[list[str]], ix: dict[str, int | None], user_id: int) -> int | None:
        uid_col = ix["uid"]
        if uid_col is None:
            return None
        target = str(user_id)
        for row_idx, row in enumerate(rows[1:], start=2):
            if self._cell(row, uid_col) == target:
                return row_idx
        return None

    def _upsert_row(
        self,
        *,
        sheet_name: str,
        user_id: int,
        phone: str,
        city: str | None,
        category: str,
        name: str | None,
    ) -> None:
        ws = self._ws(sheet_name)
        rows = ws.get_all_values()
        if not rows:
            rows = [SUPPLIER_HEADERS if sheet_name == SHEET_SUPPLIERS else CUSTOMER_HEADERS]
        ix = self._indexes(rows[0])
        ph = self._normalize_telegram_phone(phone)
        now = self._now_iso()
        existing_row = self._find_row_by_user_id(rows, ix, user_id)

        if existing_row is not None:
            row = rows[existing_row - 1]
            row_id = self._cell(row, ix["id"]) or str(existing_row - 1)
            values = [row_id, city or "", category, ph, name or "", str(user_id), now]
            ws.update(f"A{existing_row}:G{existing_row}", [values], value_input_option="RAW")
            return

        next_id = self._next_id(rows)
        ws.append_row(
            [next_id, city or "", category, ph, name or "", str(user_id), now],
            value_input_option="RAW",
        )

    def upsert_supplier(
        self,
        *,
        user_id: int,
        phone: str,
        city: str | None,
        category: str,
        name: str | None = None,
        **_kwargs: Any,
    ) -> None:
        self._upsert_row(
            sheet_name=SHEET_SUPPLIERS,
            user_id=user_id,
            phone=phone,
            city=city,
            category=category,
            name=name,
        )

    def upsert_customer(
        self,
        *,
        user_id: int,
        phone: str,
        city: str | None,
        category: str,
        name: str | None = None,
        **_kwargs: Any,
    ) -> None:
        self._upsert_row(
            sheet_name=SHEET_CUSTOMERS,
            user_id=user_id,
            phone=phone,
            city=city,
            category=category,
            name=name,
        )

    def append_supplier(self, **kwargs: Any) -> None:
        self.upsert_supplier(**kwargs)

    def append_customer(self, **kwargs: Any) -> None:
        self.upsert_customer(**kwargs)

    def get_user_profile(self, user_id: int) -> tuple[str | None, str | None]:
        best: tuple[str | None, str | None, str] | None = None
        for sheet_name in (SHEET_CUSTOMERS, SHEET_SUPPLIERS):
            ws = self._ws(sheet_name)
            rows = ws.get_all_values()
            if len(rows) < 2:
                continue
            ix = self._indexes(rows[0])
            for row in rows[1:]:
                if self._cell(row, ix["uid"]) != str(user_id):
                    continue
                phone = self._cell(row, ix["phone"]) or None
                city = self._cell(row, ix["city"]) or None
                updated = self._cell(row, ix["updated"])
                if best is None or updated >= best[2]:
                    best = (phone, city, updated)
        if best:
            return (best[0], best[1])
        return (None, None)

    def find_suppliers(self, *, query: str, city: str | None, limit: int = 30) -> list[dict]:
        ws = self._ws(SHEET_SUPPLIERS)
        rows = ws.get_all_values()
        if len(rows) < 2:
            return []

        ix = self._indexes(rows[0])
        seen: set[str] = set()
        in_city: list[dict] = []
        other: list[dict] = []

        for row in reversed(rows[1:]):
            supply = self._cell(row, ix["supply"])
            if not supply_matches(query, supply):
                continue

            phone = self._cell(row, ix["phone"])
            name = self._cell(row, ix["name"])
            key = f"{phone}|{name}"
            if key in seen:
                continue
            seen.add(key)

            row_city = self._cell(row, ix["city"])
            item = {
                "user_id": self._cell(row, ix["uid"]),
                "phone": phone,
                "city": row_city,
                "category": supply,
                "name": name,
                "source": None,
                "created_at": self._cell(row, ix["updated"]),
            }
            if city and row_city == city:
                in_city.append(item)
            else:
                other.append(item)

        return (in_city if in_city else other)[:limit]

    def export_rows(self, role: str) -> list[dict]:
        sheet_name = SHEET_SUPPLIERS if role == "supplier" else SHEET_CUSTOMERS
        ws = self._ws(sheet_name)
        rows = ws.get_all_values()
        if len(rows) < 2:
            return []

        ix = self._indexes(rows[0])
        out: list[dict] = []
        for row in rows[1:]:
            uid_raw = self._cell(row, ix["uid"])
            try:
                uid = int(uid_raw) if uid_raw else 0
            except ValueError:
                uid = 0
            out.append(
                {
                    "id": self._cell(row, ix["id"]),
                    "user_id": uid,
                    "phone": self._cell(row, ix["phone"]),
                    "city": self._cell(row, ix["city"]),
                    "category": self._cell(row, ix["supply"]),
                    "name": self._cell(row, ix["name"]),
                    "created_at": self._cell(row, ix["updated"]),
                }
            )
        return out

    def list_telegram_user_ids(self) -> list[int]:
        ids: set[int] = set()
        for sheet_name in (SHEET_SUPPLIERS, SHEET_CUSTOMERS):
            ws = self._ws(sheet_name)
            rows = ws.get_all_values()
            if len(rows) < 2:
                continue
            ix = self._indexes(rows[0])
            uid_col = ix["uid"]
            if uid_col is None:
                continue
            for row in rows[1:]:
                raw = self._cell(row, uid_col)
                if not raw:
                    continue
                try:
                    uid = int(raw)
                except ValueError:
                    continue
                if uid > 0:
                    ids.add(uid)
        return sorted(ids)

    def find_matches(self) -> list[dict]:
        ws_c = self._ws(SHEET_CUSTOMERS)
        ws_s = self._ws(SHEET_SUPPLIERS)
        crows = ws_c.get_all_values()
        srows = ws_s.get_all_values()
        if len(crows) < 2 or len(srows) < 2:
            return []

        c_ix = self._indexes(crows[0])
        s_ix = self._indexes(srows[0])

        out: list[dict] = []
        for crow in crows[1:]:
            need = self._cell(crow, c_ix["supply"])
            if not need:
                continue
            try:
                c_uid_i = int(self._cell(crow, c_ix["uid"]) or 0)
            except (TypeError, ValueError):
                continue
            if c_uid_i <= 0:
                continue
            c_phone_s = self._cell(crow, c_ix["phone"])
            for srow in srows[1:]:
                supply = self._cell(srow, s_ix["supply"])
                if not supply_matches(need, supply):
                    continue
                try:
                    s_uid_i = int(self._cell(srow, s_ix["uid"]) or 0)
                except (TypeError, ValueError):
                    s_uid_i = 0
                sp = self._cell(srow, s_ix["phone"])
                out.append(
                    {
                        "norm_category": need.casefold(),
                        "customer_category": need,
                        "customer_user_id": c_uid_i,
                        "customer_phone": c_phone_s,
                        "supplier_user_id": s_uid_i,
                        "supplier_phone": sp,
                    }
                )
        return out

    def upload_suppliers_bulk(self, rows: list[list]) -> None:
        ws = self._ws(SHEET_SUPPLIERS)
        ws.clear()
        ws.append_row(SUPPLIER_HEADERS, value_input_option="RAW")
        if rows:
            ws.append_rows(rows, value_input_option="RAW")

    def ensure_customer_sheet(self) -> None:
        ws = self._ws(SHEET_CUSTOMERS)
        if not ws.row_values(1):
            ws.append_row(CUSTOMER_HEADERS, value_input_option="RAW")
