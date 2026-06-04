"""Экспорт нормализованных данных для Google Таблиц (xlsx с 3 листами или CSV)."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from parse_ideal_table import parse_file

try:
    import openpyxl
    from openpyxl import Workbook
except Exception:  # pragma: no cover
    openpyxl = None
    Workbook = None  # type: ignore


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


def _write_csv(path: Path, headers: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(headers)
        w.writerows(rows)


def build_rows(result) -> tuple[list[list], list[list], list[list]]:
    suppliers: list[list] = []
    for i, s in enumerate(result.suppliers, start=1):
        phone = s.phone
        link = ""
        if phone.startswith("http"):
            link = phone
            phone = ""
        elif s.source:
            link = s.source
        suppliers.append(
            [
                i,
                s.city,
                s.region,
                s.section,
                s.material,
                s.category,
                s.name or "",
                phone,
                link,
                "",
                "import",
                "",
            ]
        )

    cat_names = list(result.categories)
    if "Другое" not in cat_names:
        cat_names.append("Другое")
    categories = [[n, name, 1] for n, name in enumerate(cat_names, start=1)]
    customers: list[list] = []
    return suppliers, customers, categories


def write_workbook(path: Path, suppliers: list[list], customers: list[list], categories: list[list]) -> None:
    if Workbook is None:
        raise RuntimeError("openpyxl required")

    wb = Workbook()
    ws = wb.active
    ws.title = "Поставщики"
    ws.append(SUPPLIER_HEADERS)
    for row in suppliers:
        ws.append(row)

    ws2 = wb.create_sheet("Заказчики")
    ws2.append(CUSTOMER_HEADERS)
    for row in customers:
        ws2.append(row)

    ws3 = wb.create_sheet("Категории")
    ws3.append(CATEGORY_HEADERS)
    for row in categories:
        ws3.append(row)

    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Ideal xlsx → normalized Google Sheets template")
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument(
        "--out",
        default=str(PROJECT_ROOT / "data" / "normalized_for_google.xlsx"),
        help="Output xlsx path (3 sheets)",
    )
    ap.add_argument("--csv-dir", default=None, help="Optional directory for separate CSV files")
    args = ap.parse_args()

    result = parse_file(args.inp, sheet_name=args.sheet)
    suppliers, customers, categories = build_rows(result)

    out = Path(args.out)
    write_workbook(out, suppliers, customers, categories)
    print(f"Written: {out}")
    print(f"  Поставщики: {len(suppliers)}")
    print(f"  Категории: {len(categories)}")
    print(f"  Заказчики: {len(customers)} (пусто до регистраций в боте)")

    if args.csv_dir:
        d = Path(args.csv_dir)
        _write_csv(d / "suppliers.csv", SUPPLIER_HEADERS, suppliers)
        _write_csv(d / "customers.csv", CUSTOMER_HEADERS, customers)
        _write_csv(d / "categories.csv", CATEGORY_HEADERS, categories)
        print(f"CSV: {d}")


if __name__ == "__main__":
    main()
