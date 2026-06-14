# Бот + Google Таблицы

**Полная инструкция:** [SETUP.md](SETUP.md)

```powershell
cd d:\baza
pip install -r requirements.txt
# настрой .env + credentials/google-service-account.json
python scripts/check_setup.py
python main.py
```

Данные поставщиков и заказчиков — **только в Google Таблице**. SQLite хранит сессию диалога.
