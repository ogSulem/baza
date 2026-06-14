"""Парсер «Поставщики общий список.xlsx» → строки для Google Таблиц."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    import openpyxl  # type: ignore
except Exception:  # pragma: no cover
    openpyxl = None

from contact_utils import contact_phone_field, normalize_phone

URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

PHONE_RE = re.compile(r"\b(?:\+?7|8)?\d{10}\b")

HEADER_KEYS = {
    "no": {"№", "n", "no"},
    "material": {"материал"},
    "purpose": {"назначение"},
    "product": {"товар"},
    "price": {"стоимость"},
}

SECTION_TITLES = {
    "фундамент",
    "утепление",
    "кровля",
    "окна/двери",
    "электрика",
    "банная печь",
    "печь",
    "баня",
    "сантехника",
    "крепеж",
    "грузоперевозки/аренда газели",
    "грузоперевозки",
    "товары для бани",
    "альфа",
}

REGION_RE = re.compile(r"^каркас\s+(.+)$", re.IGNORECASE)
MAX_SUPPLY_LEN = 3500


@dataclass
class NormalizedSupplier:
    city: str
    region: str
    supply: str
    name: str | None
    phone: str


@dataclass
class ParseResult:
    suppliers: list[NormalizedSupplier] = field(default_factory=list)
    cities: list[str] = field(default_factory=list)


def _cell_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if re.fullmatch(r"\d+\.0", s):
        return s[:-2]
    return s


def _header_map(cols: list[str]) -> dict[str, int]:
    m: dict[str, int] = {}
    for i, c in enumerate(cols):
        key = (c or "").strip().casefold()
        for name, variants in HEADER_KEYS.items():
            if key in variants:
                m[name] = i
    return m


def _is_numbered_row(cols: list[str]) -> bool:
    if not cols:
        return False
    first = _cell_str(cols[0]).rstrip(".0")
    return bool(first) and first.isdigit()


def _parse_region_line(line: str) -> tuple[str, str] | None:
    m = REGION_RE.match(line.strip())
    if not m:
        return None
    region = m.group(1).strip()
    return _region_to_city(region), region


def _region_to_city(region: str) -> str:
    r = region.strip()
    low = r.casefold()
    if low.startswith("каркас "):
        parts = r.split(None, 1)
        r = parts[1] if len(parts) > 1 else r
        low = r.casefold()
    if " и " in low:
        return r.split(" и ", 1)[0].strip()
    known = {
        "алтайский край": "Барнаул",
        "краснодарский край": "Краснодар",
        "башкирия": "Уфа",
        "белгород": "Белгород",
        "брянск": "Брянск",
        "воронеж": "Воронеж",
        "елец": "Елец",
        "иркутск": "Иркутск",
        "владивосток": "Владивосток",
    }
    for key, city in known.items():
        if key in low:
            return city
    parts = [p for p in re.split(r"\s+", r) if p]
    if len(parts) == 1:
        return parts[0]
    return parts[-1] if parts else r


def _is_section_title(line: str) -> bool:
    s = line.strip().casefold()
    if not s or "\t" in line:
        return False
    if len(s) > 55:
        return False
    if s in SECTION_TITLES:
        return True
    # «Утепление » with trailing space
    return s.strip() in SECTION_TITLES


def _row_contact(cols: list[str]) -> tuple[str | None, str | None, str | None]:
    cells = [_cell_str(c) for c in cols if _cell_str(c)]
    if not cells:
        return None, None, None

    name = None
    phone = None
    url = None

    if len(cells) >= 2:
        left, right = cells[-2], cells[-1]
        if URL_RE.search(left):
            url = URL_RE.search(left).group(0)
            name = right
        elif PHONE_RE.search(left):
            m = PHONE_RE.search(left)
            phone = normalize_phone(m.group(0)) if m else None
            name = right
        elif PHONE_RE.search(right):
            m = PHONE_RE.search(right)
            phone = normalize_phone(m.group(0)) if m else None
            name = left
        elif URL_RE.search(right):
            url = URL_RE.search(right).group(0)
            name = left
        else:
            # Имя | телефон (Владивосток)
            if PHONE_RE.search(right) or URL_RE.search(right):
                m = PHONE_RE.search(right) or URL_RE.search(right)
                if m and PHONE_RE.search(right):
                    phone = normalize_phone(m.group(0))
                elif URL_RE.search(right):
                    url = URL_RE.search(right).group(0)
                name = left
            else:
                name = right
                if PHONE_RE.search(left):
                    phone = normalize_phone(PHONE_RE.search(left).group(0))

    if not phone and not url:
        for c in cells:
            if URL_RE.search(c):
                url = URL_RE.search(c).group(0)
            elif PHONE_RE.search(c):
                m = PHONE_RE.search(c)
                if m and not phone:
                    phone = normalize_phone(m.group(0))

    if not name:
        for c in reversed(cells):
            if c in {phone or "", url or ""}:
                continue
            if not PHONE_RE.search(c) and not URL_RE.search(c):
                name = c
                break

    return phone, name, url


def _extract_field(cols: list[str], col_map: dict[str, int], key: str) -> str:
    idx = col_map.get(key)
    if idx is None or idx >= len(cols):
        return ""
    return _cell_str(cols[idx])


def _trim_leading_empty(cols: list[str]) -> list[str]:
    out = list(cols)
    while len(out) > 1 and not _cell_str(out[0]):
        out.pop(0)
    return out


def _build_supply(
    *,
    section: str,
    material: str,
    purpose: str,
    products: list[str],
) -> str:
    parts: list[str] = []
    if section:
        parts.append(f"Раздел: {section}")
    if material:
        parts.append(f"Материал: {material}")
    if purpose:
        parts.append(f"Назначение: {purpose}")
    for p in products:
        p = p.strip()
        if p:
            parts.append(f"Товар: {p}")
    text = " | ".join(parts)
    if len(text) > MAX_SUPPLY_LEN:
        text = text[: MAX_SUPPLY_LEN - 3] + "..."
    return text or purpose or material or section or "Поставщик"


def parse_rows(rows: list[list[str]]) -> ParseResult:
    result = ParseResult()
    col_map: dict[str, int] = {}

    city = ""
    region = ""
    section = ""
    material = ""
    purpose = ""
    block_products: list[str] = []
    in_contacts = False

    cities_seen: set[str] = set()

    def reset_block(*, mat: str = "", pur: str = "") -> None:
        nonlocal material, purpose, block_products
        material = mat
        purpose = pur
        block_products = []

    def append_product(product: str, price: str = "") -> None:
        product = product.strip()
        if not product:
            return
        line = product
        if price:
            line = f"{product} — {price.strip()}"
        if line not in block_products:
            block_products.append(line)

    def add_supplier(*, phone: str | None, name: str | None, url: str | None) -> None:
        if not city:
            return
        ph = contact_phone_field(phone=phone, url=url)
        if not ph:
            return
        supply = _build_supply(
            section=section,
            material=material,
            purpose=purpose,
            products=block_products,
        )
        result.suppliers.append(
            NormalizedSupplier(
                city=city,
                region=region,
                supply=supply,
                name=(name or "").strip() or None,
                phone=ph,
            )
        )
        cities_seen.add(city)

    for cols in rows:
        cells = _trim_leading_empty([_cell_str(c) for c in cols])
        while cells and not cells[-1]:
            cells.pop()
        if not any(cells):
            continue

        line = "\t".join(cells)
        low = line.casefold()
        first_nonempty = next((c for c in cells if c.strip()), "")

        if "контакты поставщиков" in low:
            in_contacts = True
            phone, name, url = _row_contact(cells)
            if phone or url:
                add_supplier(phone=phone, name=name, url=url)
            continue

        hm = _header_map(cells)
        if hm:
            col_map = hm
            in_contacts = False
            continue

        region_hit = _parse_region_line(first_nonempty) or _parse_region_line(cells[0] if cells else "")
        if region_hit:
            city, region = region_hit
            in_contacts = False
            reset_block()
            section = ""
            cities_seen.add(city)
            continue

        if _is_section_title(first_nonempty or line):
            section = (first_nonempty or line).strip()
            in_contacts = False
            reset_block()
            continue

        if _is_numbered_row(cells):
            in_contacts = False
            mat = _extract_field(cells, col_map, "material") or (cells[1] if len(cells) > 1 else "")
            pur = _extract_field(cells, col_map, "purpose")
            reset_block(mat=mat, pur=pur)
            prod = _extract_field(cells, col_map, "product")
            price = _extract_field(cells, col_map, "price")
            append_product(prod, price)
            phone, name, url = _row_contact(cells)
            if phone or url:
                add_supplier(phone=phone, name=name, url=url)
            continue

        if in_contacts:
            if _is_numbered_row(cells):
                in_contacts = False
                mat = _extract_field(cells, col_map, "material") or (cells[1] if len(cells) > 1 else "")
                pur = _extract_field(cells, col_map, "purpose")
                reset_block(mat=mat, pur=pur)
                prod = _extract_field(cells, col_map, "product")
                price = _extract_field(cells, col_map, "price")
                append_product(prod, price)
                continue

            phone, name, url = _row_contact(cells)
            if phone or url:
                add_supplier(phone=phone, name=name, url=url)
            continue

        prod = _extract_field(cells, col_map, "product") or (cells[3] if len(cells) > 3 else "")
        price = _extract_field(cells, col_map, "price") or (cells[4] if len(cells) > 4 else "")

        phone, name, url = _row_contact(cells)
        if phone or url:
            add_supplier(phone=phone, name=name, url=url)
            continue

        if prod:
            append_product(prod, price)

    result.cities = sorted(cities_seen)
    return result


def xlsx_rows(path: str, *, sheet_name: str | None = None) -> list[list[str]]:
    if openpyxl is None:
        raise RuntimeError("openpyxl is not installed")

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active
    out: list[list[str]] = []
    for row in ws.iter_rows(values_only=True):
        vals = [_cell_str(v) for v in row]
        while vals and not vals[-1]:
            vals.pop()
        if any(v.strip() for v in vals):
            out.append(vals)
    return out


def parse_file(path: str, *, sheet_name: str | None = None) -> ParseResult:
    p = Path(path)
    if p.suffix.lower() == ".xlsx":
        return parse_rows(xlsx_rows(str(p), sheet_name=sheet_name))
    content = p.read_text(encoding="utf-8")
    rows = [line.rstrip("\n").split("\t") for line in content.splitlines() if line.strip()]
    return parse_rows(rows)


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--limit", type=int, default=5)
    args = ap.parse_args()

    res = parse_file(args.inp, sheet_name=args.sheet)
    print(f"Городов: {len(res.cities)}")
    print(f"Поставщиков: {len(res.suppliers)}")
    for s in res.suppliers[: args.limit]:
        print(f"\n[{s.city}] {s.name or '-'} | {s.phone[:40]}")
        print(f"  {s.supply[:120]}...")


if __name__ == "__main__":
    main()
