import json
import os
import re
from urllib.request import Request, urlopen


def fetch_json(url: str):
    req = Request(url, headers={"User-Agent": "baza-bot/1.0"})
    with urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))


def norm(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def main() -> int:
    ru_url = os.getenv(
        "RU_CITIES_URL",
        "https://raw.githubusercontent.com/arbaev/russia-cities/master/russia-cities.json",
    )
    # Raw gist URL is revision-specific; keep a pinned revision for stability.
    by_url = os.getenv(
        "BY_CITIES_URL",
        "https://gist.githubusercontent.com/alex-oleshkevich/6946d85bf075a6049027306538629794/raw/3986e8e1ade2d4e1186f8fee719960de32ac6955/by-cities.json",
    )
    kz_url = os.getenv("KZ_CITIES_URL", "https://namaztimes.kz/ru/api/cities?id=99")

    ru = fetch_json(ru_url)
    by = fetch_json(by_url)
    kz = fetch_json(kz_url)

    out: list[str] = []

    for item in ru:
        name = norm(item.get("name") or "")
        if name:
            out.append(name)

    if isinstance(by, list):
        for item in by:
            if isinstance(item, str):
                name = norm(item)
            else:
                obj = item or {}
                name = norm(obj.get("name") or obj.get("city") or obj.get("title") or "")
            if name:
                out.append(name)
    elif isinstance(by, dict):
        for _k, v in by.items():
            name = norm(str(v))
            if name:
                out.append(name)

    if isinstance(kz, dict):
        for _k, v in kz.items():
            name = norm(str(v))
            if name:
                out.append(name)
    elif isinstance(kz, list):
        for item in kz:
            if isinstance(item, str):
                name = norm(item)
            else:
                obj = item or {}
                name = norm(obj.get("name") or obj.get("city") or obj.get("title") or "")
            if name:
                out.append(name)

    unique = sorted(set(out), key=lambda s: s.casefold())

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(root, "data")
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, "cities_ru_kz_by.json")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    print(f"Wrote {len(unique)} cities to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
