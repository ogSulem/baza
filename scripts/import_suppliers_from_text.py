from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import Database

try:
    import openpyxl  # type: ignore
except Exception:  # pragma: no cover
    openpyxl = None


PHONE_RE = re.compile(r"\b(?:\+?7|8)?\d{10}\b")
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


def normalize_phone(raw: str) -> str | None:
    s = (raw or "").strip()
    s = re.sub(r"[^0-9+]", "", s)
    digits = re.sub(r"\D", "", s)

    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) != 11:
        return None
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    return "+" + digits


def parse_contacts_line(line: str) -> tuple[str | None, str | None, str | None]:
    line = (line or "").strip()
    if not line:
        return None, None, None

    url = None
    m_url = URL_RE.search(line)
    if m_url:
        url = m_url.group(0).strip()

    phone = None
    m_phone = PHONE_RE.search(line)
    if m_phone:
        phone = normalize_phone(m_phone.group(0))

    name = None
    parts = [p.strip() for p in re.split(r"\t+", line) if p.strip()]
    if len(parts) >= 2:
        # Try infer which part is contact and which part is name.
        # Common cases:
        # 1) phone<TAB>name
        # 2) url<TAB>name
        # 3) name<TAB>phone
        # 4) name<TAB>url
        url_part = None
        phone_part = None
        for p in parts:
            if url_part is None and URL_RE.search(p):
                url_part = p
            if phone_part is None and PHONE_RE.search(p):
                phone_part = p

        if url_part and not url:
            m = URL_RE.search(url_part)
            url = m.group(0).strip() if m else url
        if phone_part and not phone:
            m = PHONE_RE.search(phone_part)
            phone = normalize_phone(m.group(0)) if m else phone

        # Name is the first part that is not the detected url/phone part
        for p in parts:
            if url_part and p == url_part:
                continue
            if phone_part and p == phone_part:
                continue
            name = p
            break

        # Fallback: keep previous behavior
        if not name:
            name = parts[-1]
    else:
        if url:
            # try split by url
            rest = line.replace(url, " ").strip()
            rest = re.sub(r"\s+", " ", rest)
            if rest:
                name = rest
        elif phone:
            rest = line.replace(m_phone.group(0), " ").strip() if m_phone else ""
            rest = re.sub(r"\s+", " ", rest)
            if rest:
                name = rest

    source = url or (phone or None)
    return phone, name, source


def iter_tsv_lines(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n").rstrip("\r")
        if not line.strip():
            continue
        rows.append(line.split("\t"))
    return rows


def xlsx_rows(path: str, *, sheet_name: str | None) -> list[list[str]]:
    if openpyxl is None:
        raise RuntimeError("openpyxl is not installed. Run: pip install -r requirements.txt")

    wb = openpyxl.load_workbook(path, data_only=True)
    if sheet_name:
        ws = wb[sheet_name]
    else:
        ws = wb.active

    out: list[list[str]] = []
    for row in ws.iter_rows(values_only=True):
        vals: list[str] = []
        for v in row:
            if v is None:
                vals.append("")
            else:
                vals.append(str(v).strip())
        # trim trailing empty cells
        while vals and not vals[-1]:
            vals.pop()
        if not any(c.strip() for c in vals):
            continue
        out.append(vals)
    return out


async def run(*, db_path: str, input_path: str, default_city: str | None, sheet: str | None) -> None:
    db = Database(db_path)
    await db.init()

    path = str(input_path)
    if path.lower().endswith(".xlsx"):
        rows = xlsx_rows(path, sheet_name=sheet)
    else:
        content = Path(input_path).read_text(encoding="utf-8")
        rows = iter_tsv_lines(content)

    current_category: str | None = None
    in_contacts = False
    material_col: int | None = None

    imported = 0
    skipped = 0

    for cols in rows:
        line = "\t".join(cols).strip()
        low = line.casefold()

        if low.startswith("контакты поставщиков"):
            in_contacts = True
            continue

        # New section or row that looks like header/category row
        if not in_contacts:
            # Try detect header row to learn where "Материал" column is
            if material_col is None:
                for i, c in enumerate(cols):
                    if (c or "").strip().casefold() == "материал":
                        material_col = i
                        break

            # Table row: first cell is numeric index, and "Материал" column is usually 2nd
            if cols and cols[0].strip().isdigit() and len(cols) >= 2:
                mat = None
                if material_col is not None and material_col < len(cols):
                    mat = cols[material_col].strip()
                else:
                    mat = cols[1].strip()
                if mat:
                    current_category = mat
            else:
                # Section titles like "Фундамент", "Кровля" etc.
                # If a line contains no tabs and is short -> treat as category fallback.
                if "\t" not in line and 1 <= len(line) <= 40:
                    # ignore obvious headers
                    if line not in {"№", "Материал", "Назначение", "Товар", "Стоимость"}:
                        current_category = line.strip()
            continue

        # contacts block
        if in_contacts:
            # contacts end when a new numbered row starts or a new section title starts
            if cols and cols[0].strip().isdigit():
                in_contacts = False
                # This row also can define a new category
                mat = None
                if material_col is not None and material_col < len(cols):
                    mat = cols[material_col].strip()
                elif len(cols) >= 2:
                    mat = cols[1].strip()
                if mat:
                    current_category = mat
                continue

            phone, name, source = parse_contacts_line(line)
            if not phone and not source:
                skipped += 1
                continue

            # Keep a single contact column semantics: if we have only URL, store it into phone.
            if not phone and source:
                phone = source
                source = None

            category = (current_category or "").strip()
            if not category:
                skipped += 1
                continue

            city = default_city

            # user_id=0 means imported/outside telegram
            await db.add_supplier(
                user_id=0,
                phone=(phone or ""),
                city=city,
                category=category,
                name=name,
                source=source,
            )
            imported += 1

    print(f"Imported suppliers: {imported}. Skipped lines: {skipped}.")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True, help="Path to sqlite DB, e.g. data.db")
    p.add_argument(
        "--in",
        dest="inp",
        required=True,
        help="Input UTF-8 TSV text file (copy/paste from Excel) or .xlsx file",
    )
    p.add_argument("--sheet", default=None, help="Optional .xlsx sheet name (default: active sheet)")
    p.add_argument("--city", default=None, help="Optional city to assign to all imported suppliers")
    args = p.parse_args()

    asyncio.run(run(db_path=args.db, input_path=args.inp, default_city=args.city, sheet=args.sheet))


if __name__ == "__main__":
    main()
