"""Проверка настройки перед запуском бота."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def main() -> None:
    print("=== Проверка настройки бота ===\n")
    ok = True

    try:
        from config import load_config

        cfg = load_config()
        print("[OK] .env загружен")
        print(f"     BOT_TOKEN: задан")
        print(f"     ADMIN_IDS: {len(cfg.admin_ids)} шт.")
        print(f"     GOOGLE_SPREADSHEET_ID: {cfg.google_spreadsheet_id[:8]}...")
        print(f"     GOOGLE_CREDENTIALS_JSON: {cfg.google_credentials_path}")
    except Exception as e:
        print(f"[FAIL] config: {e}")
        ok = False
        print("\nИсправьте .env и credentials, затем запустите снова.")
        raise SystemExit(1)

    try:
        from google_sheets import GoogleSheetsStore, SheetsConfig

        store = GoogleSheetsStore(
            SheetsConfig(
                spreadsheet_id=cfg.google_spreadsheet_id,
                credentials_path=cfg.google_credentials_path,
            )
        )
        info = store.verify_connection()
        print(f"[OK] Google Sheets: {info}")
    except Exception as e:
        print(f"[FAIL] Google Sheets: {e}")
        print("\nЧастые причины:")
        print("  - JSON ключ пустой или битый (credentials/google-service-account.json)")
        print("  - Таблица не расшарена на client_email из JSON (роль: Редактор)")
        print("  - Неверный GOOGLE_SPREADSHEET_ID")
        print("  - Google Sheets API не включён в Google Cloud")
        ok = False

    try:
        from aiogram import Bot
        import asyncio

        async def ping():
            bot = Bot(cfg.bot_token)
            me = await bot.get_me()
            await bot.session.close()
            return me.username

        username = asyncio.run(ping())
        print(f"[OK] Telegram bot: @{username}")
    except Exception as e:
        print(f"[WARN] Telegram: {e}")
        print("     (нет сети или неверный BOT_TOKEN — проверьте вручную)")

    print()
    if ok:
        print("Всё готово. Запуск: python main.py")
    else:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
