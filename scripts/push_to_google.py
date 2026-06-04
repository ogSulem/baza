"""Загрузить нормализованные данные из xlsx в Google Таблицу."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from export_normalized import build_rows  # type: ignore
from parse_ideal_table import parse_file
from google_sheets import GoogleSheetsStore, SheetsConfig


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="Поставщики общий список.xlsx")
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--spreadsheet-id", default=os.getenv("GOOGLE_SPREADSHEET_ID", "").strip())
    ap.add_argument(
        "--credentials",
        default=os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip(),
        help="Service account JSON path",
    )
    args = ap.parse_args()

    if not args.spreadsheet_id or not args.credentials:
        raise SystemExit("Set GOOGLE_SPREADSHEET_ID and GOOGLE_CREDENTIALS_JSON in .env")

    result = parse_file(args.inp, sheet_name=args.sheet)
    names = list(result.categories)
    if "Другое" not in names:
        names.append("Другое")

    suppliers, _, categories = build_rows(result)
    store = GoogleSheetsStore(
        SheetsConfig(spreadsheet_id=args.spreadsheet_id, credentials_path=args.credentials)
    )
    store.upload_suppliers_bulk(suppliers)
    store.replace_categories(names)

    print(f"Uploaded suppliers: {len(suppliers)}")
    print(f"Uploaded categories: {len(names)}")


if __name__ == "__main__":
    main()
