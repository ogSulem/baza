"""Залить поставщиков из xlsx в Google Таблицу."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from export_normalized import build_supplier_rows
from google_sheets import GoogleSheetsStore, SheetsConfig
from parse_ideal_table import parse_file


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--spreadsheet-id", default=os.getenv("GOOGLE_SPREADSHEET_ID", "").strip())
    ap.add_argument("--credentials", default=os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip())
    args = ap.parse_args()

    if not args.spreadsheet_id or not args.credentials:
        raise SystemExit("В .env укажите GOOGLE_SPREADSHEET_ID и GOOGLE_CREDENTIALS_JSON")

    from config import resolve_project_path

    creds_path = resolve_project_path(args.credentials)

    result = parse_file(args.inp, sheet_name=args.sheet)
    rows = build_supplier_rows(result)
    store = GoogleSheetsStore(
        SheetsConfig(spreadsheet_id=args.spreadsheet_id, credentials_path=creds_path)
    )
    store.upload_suppliers_bulk(rows)
    store.ensure_customer_sheet()
    print(f"Готово: {len(rows)} поставщиков в лист «Поставщики», лист «Заказчики» с шапкой.")


if __name__ == "__main__":
    main()
