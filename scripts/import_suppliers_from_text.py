"""Импорт из идеальной таблицы в SQLite (legacy) или подсказка использовать Google."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from db import Database
from parse_ideal_table import parse_file


async def run(*, db_path: str, input_path: str, sheet: str | None, clear_imported: bool) -> None:
    db = Database(db_path)
    await db.init()

    if clear_imported:
        conn = await db.connect()
        try:
            await conn.execute("DELETE FROM suppliers WHERE user_id = 0")
            await conn.commit()
        finally:
            await conn.close()

    result = parse_file(input_path, sheet_name=sheet)
    imported = 0
    for s in result.suppliers:
        phone = s.phone
        source = s.source
        if phone.startswith("http"):
            source, phone = phone, ""
        await db.add_supplier(
            user_id=0,
            phone=phone or source or "",
            city=s.city,
            category=s.category,
            name=s.name,
            source=source,
        )
        imported += 1

    await db.replace_categories([(name, i) for i, name in enumerate(result.categories, start=1)])
    print(f"Imported suppliers: {imported}. Categories: {len(result.categories)}.")
    print("Для Google Таблиц используйте: python scripts/export_normalized.py или push_to_google.py")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--sheet", default=None)
    p.add_argument("--clear-imported", action="store_true")
    args = p.parse_args()
    asyncio.run(
        run(
            db_path=args.db,
            input_path=args.inp,
            sheet=args.sheet,
            clear_imported=args.clear_imported,
        )
    )


if __name__ == "__main__":
    main()
