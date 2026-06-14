"""Импорт xlsx в SQLite (без Google)."""

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
    for s in result.suppliers:
        await db.add_supplier(
            user_id=0,
            phone=s.phone,
            city=s.city,
            category=s.supply,
            name=s.name,
            source=None,
        )
    print(f"Imported: {len(result.suppliers)}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--in", dest="inp", required=True)
    p.add_argument("--sheet", default=None)
    p.add_argument("--clear-imported", action="store_true")
    args = p.parse_args()
    asyncio.run(
        run(db_path=args.db, input_path=args.inp, sheet=args.sheet, clear_imported=args.clear_imported)
    )


if __name__ == "__main__":
    main()
