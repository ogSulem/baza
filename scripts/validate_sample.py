"""Проверка конвертации на примере Алтай / первый блок."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from parse_ideal_table import parse_file

OUT = Path(__file__).resolve().parents[1] / "data" / "validate.txt"


def main() -> None:
    p = next(Path(__file__).resolve().parents[1].glob("*.xlsx"))
    res = parse_file(str(p))
    lines: list[str] = []

    alt = [s for s in res.suppliers if "алтай" in s.region.casefold()][:12]
    lines.append("=== Алтайский край, первые контакты ===")
    for s in alt:
        lines.append(f"{s.name} | {s.phone[:80]}")
        lines.append(f"  {s.supply[:200]}")
        lines.append("")

    ivan = [
        s
        for s in res.suppliers
        if s.name and "иван" in s.name.casefold() and "+79831779695" in s.phone
    ]
    lines.append("Иван +79831779695: " + ("OK" if ivan else "FAIL"))
    if ivan:
        lines.append(ivan[0].supply[:400])

    alex = [s for s in res.suppliers if s.phone.startswith("http") and "avito" in s.phone]
    lines.append(f"\nСсылка вместо телефона: {len(alex)} поставщиков")
    if alex:
        lines.append(f"  пример: {alex[0].name} | {alex[0].phone[:60]}...")

    lines.append(f"\nВсего: {len(res.suppliers)} поставщиков, {len(res.cities)} городов")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"OK -> {OUT}")


if __name__ == "__main__":
    main()
