"""Parse «идеальную» таблицу (Поставщики общий список.xlsx) в нормализованные строки."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

try:
    import openpyxl  # type: ignore
except Exception:  # pragma: no cover
    openpyxl = None

from contact_utils import normalize_phone, parse_contacts_line

PHONE_RE = re.compile(r"\b(?:\+?7|8)?\d{10}\b")
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

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
}

REGION_RE = re.compile(r"^каркас\s+(.+)$", re.IGNORECASE)


@dataclass
class NormalizedSupplier:
    city: str
    region: str
    section: str
    material: str
    category: str
    name: str | None
    phone: str
    source: str | None


@dataclass
class NormalizedProduct:
    city: str
    region: str
    section: str
    material: str
    category: str
    product: str
    price: str


@dataclass
class ParseResult:
    suppliers: list[NormalizedSupplier] = field(default_factory=list)
    products: list[NormalizedProduct] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    cities: list[str] = field(default_factory=list)


def _cell_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


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
    first = _cell_str(cols[0])
    if not first:
        return False
    first = first.rstrip(".0") if first.endswith(".0") else first
    return first.isdigit()


def _parse_region_line(line: str) -> tuple[str, str] | None:
    m = REGION_RE.match(line.strip())
    if not m:
        return None
    region = m.group(1).strip()
    city = _region_to_city(region)
    return city, region


def _region_to_city(region: str) -> str:
    """Best-effort city for bot matching."""
    r = region.strip()
    low = r.casefold()

    # «Каркас Барнаул» → Барнаул
    if low.startswith("каркас "):
        r = r.split(None, 1)[-1] if len(r.split()) > 1 else r

    # Explicit city at start before «и … край»
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

    # Last token often is city name
    parts = [p for p in re.split(r"\s+", r) if p]
    if len(parts) == 1:
        return parts[0]
    if parts and parts[0].casefold() == "каркас" and len(parts) > 1:
        return parts[1]
    return parts[-1] if parts else r


def _is_section_title(line: str) -> bool:
    s = line.strip().casefold()
    if not s or "\t" in line:
        return False
    if len(s) > 50 or any(ch.isdigit() for ch in s):
        return False
    return s in SECTION_TITLES


def _row_contact(cols: list[str]) -> tuple[str | None, str | None, str | None]:
    cells = [_cell_str(c) for c in cols if _cell_str(c)]
    if not cells:
        return None, None, None

    # Typical Excel layout: ... | phone | name  OR  ... | url | name
    name = None
    phone = None
    source = None

    if len(cells) >= 2:
        left, right = cells[-2], cells[-1]
        if URL_RE.search(left):
            source = URL_RE.search(left).group(0)
            name = right
        elif PHONE_RE.fullmatch(re.sub(r"[^\d+]", "", left)) or PHONE_RE.search(left):
            m = PHONE_RE.search(left)
            phone = normalize_phone(m.group(0)) if m else None
            name = right
        elif PHONE_RE.search(right):
            m = PHONE_RE.search(right)
            phone = normalize_phone(m.group(0)) if m else None
            name = left
        elif URL_RE.search(right):
            source = URL_RE.search(right).group(0)
            name = left
        else:
            name = right
            if PHONE_RE.search(left):
                phone = normalize_phone(PHONE_RE.search(left).group(0))

    if not phone and not source:
        for c in cells:
            if URL_RE.search(c):
                source = URL_RE.search(c).group(0)
            elif PHONE_RE.search(c) and not URL_RE.search(c):
                m = PHONE_RE.search(c)
                if m and not phone:
                    phone = normalize_phone(m.group(0))

    if not name:
        for c in reversed(cells):
            if c == (phone or "") or c == (source or ""):
                continue
            if not PHONE_RE.search(c) and not URL_RE.search(c):
                name = c
                break

    return phone, name, source


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


def parse_rows(rows: list[list[str]]) -> ParseResult:
    result = ParseResult()
    col_map: dict[str, int] = {}

    city = ""
    region = ""
    section = ""
    material = ""
    category = ""
    in_contacts = False

    categories_seen: set[str] = set()
    cities_seen: set[str] = set()

    def add_supplier(*, phone: str, name: str | None, source: str | None) -> None:
        cat = (category or section or material or "Другое").strip()
        if not cat or not city:
            return
        if not phone and not source:
            return
        ph = phone or ""
        src = source
        if not ph and src:
            ph = src
            src = None
        result.suppliers.append(
            NormalizedSupplier(
                city=city,
                region=region,
                section=section,
                material=material,
                category=cat,
                name=name,
                phone=ph,
                source=src,
            )
        )
        categories_seen.add(cat)
        cities_seen.add(city)

    def add_product(product: str, price: str) -> None:
        if not product or not city:
            return
        result.products.append(
            NormalizedProduct(
                city=city,
                region=region,
                section=section,
                material=material,
                category=category or section,
                product=product,
                price=price,
            )
        )

    for cols in rows:
        cells = _trim_leading_empty([_cell_str(c) for c in cols])
        while cells and not cells[-1]:
            cells.pop()
        if not any(cells):
            continue

        line = "\t".join(cells)
        low = line.casefold()
        first_nonempty = next((c for c in cells if c.strip()), "")

        if low.startswith("контакты поставщиков") or first_nonempty.casefold().startswith("контакты поставщиков"):
            in_contacts = True
            phone, name, source = _row_contact(cells)
            if phone or source:
                add_supplier(phone=phone or "", name=name, source=source)
            continue

        # Header row
        hm = _header_map(cells)
        if hm:
            col_map = hm
            continue

        # Region: «Каркас Барнаул» (often single cell after empty col A)
        region_hit = _parse_region_line(first_nonempty)
        if region_hit is None:
            region_hit = _parse_region_line(cells[0])
        if region_hit:
            city, region = region_hit
            in_contacts = False
            cities_seen.add(city)
            continue

        if _is_section_title(first_nonempty or line):
            section = (first_nonempty or line).strip()
            in_contacts = False
            continue

        if _is_numbered_row(cells):
            in_contacts = False
            material = _extract_field(cells, col_map, "material") or (cells[1] if len(cells) > 1 else "")
            purpose = _extract_field(cells, col_map, "purpose")
            if purpose:
                category = purpose
                categories_seen.add(category)
            product = _extract_field(cells, col_map, "product")
            price = _extract_field(cells, col_map, "price")
            if product:
                add_product(product, price)
            phone, name, source = _row_contact(cells)
            if phone or source:
                add_supplier(phone=phone or "", name=name, source=source)
            continue

        if in_contacts:
            if _is_numbered_row(cells):
                in_contacts = False
                material = _extract_field(cells, col_map, "material") or (cells[1] if len(cells) > 1 else "")
                purpose = _extract_field(cells, col_map, "purpose")
                if purpose:
                    category = purpose
                continue

            phone, name, source = _row_contact(cells)
            if phone or source:
                add_supplier(phone=phone or "", name=name, source=source)
            continue

        # Product continuation row (no №)
        product = _extract_field(cells, col_map, "product") or (cells[3] if len(cells) > 3 else "")
        price = _extract_field(cells, col_map, "price") or (cells[4] if len(cells) > 4 else "")
        if product:
            add_product(product, price)
            # Bashkiria: contact embedded in product row
            phone, name, source = _row_contact(cells)
            if phone or source:
                add_supplier(phone=phone or "", name=name, source=source)
            continue

        # Inline contact without «Контакты» block
        phone, name, source = _row_contact(cells)
        if phone or source:
            add_supplier(phone=phone or "", name=name, source=source)

    result.categories = sorted(categories_seen)
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
    import json

    ap = argparse.ArgumentParser(description="Parse ideal supplier table → normalized JSON/stats")
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--json", default=None, help="Write full result to JSON file")
    ap.add_argument("--limit", type=int, default=5, help="Sample suppliers to print")
    args = ap.parse_args()

    res = parse_file(args.inp, sheet_name=args.sheet)
    print(f"Cities: {len(res.cities)}")
    print(f"Categories: {len(res.categories)}")
    print(f"Suppliers: {len(res.suppliers)}")
    print(f"Products: {len(res.products)}")
    print("Categories:", ", ".join(res.categories[:20]), ("..." if len(res.categories) > 20 else ""))
    for s in res.suppliers[: args.limit]:
        print(f"  [{s.city}] {s.category} | {s.name or '-'} | {s.phone[:20]}...")
    if args.json:
        Path(args.json).write_text(
            json.dumps(
                {
                    "cities": res.cities,
                    "categories": res.categories,
                    "suppliers": [s.__dict__ for s in res.suppliers],
                    "products": [p.__dict__ for p in res.products],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
