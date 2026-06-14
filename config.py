from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Config:
    bot_token: str
    admin_ids: set[int]
    db_path: str
    cities: dict[str, str]
    google_spreadsheet_id: str
    google_credentials_path: str


def resolve_project_path(raw: str) -> str:
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return str(path.resolve())


def load_config() -> Config:
    load_dotenv(PROJECT_ROOT / ".env")

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set in .env")

    raw_admin_ids = os.getenv("ADMIN_IDS", "").strip()
    admin_ids: set[int] = set()
    if raw_admin_ids:
        for part in raw_admin_ids.split(","):
            part = part.strip()
            if part:
                admin_ids.add(int(part))

    db_path = os.getenv("DB_PATH", "data/data.db").strip() or "data/data.db"
    if not os.path.isabs(db_path):
        try:
            if os.path.isdir("/data"):
                db_path = os.path.join("/data", os.path.basename(db_path))
            else:
                db_path = str(PROJECT_ROOT / db_path)
        except Exception:
            db_path = str(PROJECT_ROOT / db_path)
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

    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError(
            "GOOGLE_SPREADSHEET_ID is not set in .env — создайте Google Таблицу и укажите ID из URL"
        )

    credentials_raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not credentials_raw:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON is not set in .env — укажите путь к JSON ключу сервисного аккаунта"
        )
    credentials_path = resolve_project_path(credentials_raw)
    if not os.path.isfile(credentials_path):
        raise RuntimeError(f"Google credentials file not found: {credentials_path}")
    try:
        with open(credentials_path, encoding="utf-8") as f:
            creds = json.load(f)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Google credentials file is not valid JSON: {credentials_path}") from e
    if "client_email" not in creds or "private_key" not in creds:
        raise RuntimeError(
            f"Google credentials file is incomplete (need client_email and private_key): {credentials_path}"
        )

    return Config(
        bot_token=bot_token,
        admin_ids=admin_ids,
        db_path=db_path,
        cities=cities,
        google_spreadsheet_id=spreadsheet_id,
        google_credentials_path=credentials_path,
    )
