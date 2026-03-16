from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    bot_token: str
    admin_ids: set[int]
    db_path: str
    cities: dict[str, str]


def load_config() -> Config:
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set")

    raw_admin_ids = os.getenv("ADMIN_IDS", "").strip()
    admin_ids: set[int] = set()
    if raw_admin_ids:
        for part in raw_admin_ids.split(","):
            part = part.strip()
            if part:
                admin_ids.add(int(part))

    db_path = os.getenv("DB_PATH", "data.db").strip() or "data.db"
    # Docker-friendly default: if DB_PATH is relative and /data is mounted, store DB under /data
    if not os.path.isabs(db_path):
        try:
            if os.path.isdir("/data"):
                db_path = os.path.join("/data", db_path)
        except Exception:
            pass
    try:
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    except Exception:
        pass

    raw_cities = os.getenv("CITIES", "").strip()
    cities: dict[str, str] = {}
    if raw_cities:
        for part in raw_cities.split(","):
            name = part.strip()
            if name:
                cities[name.casefold()] = name

    return Config(bot_token=bot_token, admin_ids=admin_ids, db_path=db_path, cities=cities)
