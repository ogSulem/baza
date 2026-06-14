"""xlsx «Поставщики общий список» → файл для Google Таблиц."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).parent))

from parse_ideal_table import parse_file

try:
    from openpyxl import Workbook
except Exception:  # pragma: no cover
    Workbook = None  # type: ignore

SUPPLIER_HEADERS = ["id", "город", "что_поставляет", "телефон", "имя", "telegram_user_id", "обновлено"]
CUSTOMER_HEADERS = ["id", "город", "что_нужно", "телефон", "имя", "telegram_user_id", "обновлено"]


def build_supplier_rows(result) -> list[list]:
    rows: list[list] = []
    for i, s in enumerate(result.suppliers, start=1):
        rows.append(
            [
                i,
                s.city,
                s.supply,
                s.phone,
                s.name or "",
                "",
                "",
            ]
        )
    return rows


def write_workbook(path: Path, suppliers: list[list]) -> None:
    if Workbook is None:
        raise RuntimeError("pip install openpyxl")

    wb = Workbook()
    ws = wb.active
    ws.title = "Поставщики"
    ws.append(SUPPLIER_HEADERS)
    for row in suppliers:
        ws.append(row)

    ws2 = wb.create_sheet("Заказчики")
    ws2.append(CUSTOMER_HEADERS)

    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--out", default=str(PROJECT_ROOT / "data" / "google_suppliers.xlsx"))
    ap.add_argument("--csv", default=None, help="Папка для CSV")
    args = ap.parse_args()

    result = parse_file(args.inp, sheet_name=args.sheet)
    suppliers = build_supplier_rows(result)

    out = Path(args.out)
    write_workbook(out, suppliers)
    print(f"Файл: {out}")
    print(f"Поставщиков: {len(suppliers)}")

    if args.csv:
        d = Path(args.csv)
        d.mkdir(parents=True, exist_ok=True)
        with (d / "Поставщики.csv").open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(SUPPLIER_HEADERS)
            w.writerows(suppliers)
        with (d / "Заказчики.csv").open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(CUSTOMER_HEADERS)
        print(f"CSV: {d}")


if __name__ == "__main__":
    main()
