# Полная инструкция: бот + Google Таблицы

Бот хранит **всех поставщиков и заказчиков в Google Таблице**.  
Локально (SQLite) — только **сессия диалога** (номер, город, шаг регистрации) и админ-категории.

---

## Как это работает

| Что | Где хранится |
|-----|--------------|
| База поставщиков (~1400 из прайса + новые из бота) | Google → лист **Поставщики** |
| Заказчики из бота | Google → лист **Заказчики** |
| Номер/город во время диалога | SQLite → `pending_users` (временно) |
| Поиск поставщиков для заказчика | Google → лист **Поставщики** |
| Админ-выгрузка CSV | Google |
| Рассылка | Telegram ID из Google (кто регистрировался через бота) |

Повторная регистрация того же пользователя **обновляет строку** (по `telegram_user_id`), а не дублирует.

---

## Шаг 0. Что нужно заранее

- Python 3.12+ (или Docker)
- Аккаунт Google
- Telegram-аккаунт

---

## Шаг 1. Telegram-бот

1. Открой [@BotFather](https://t.me/BotFather) в Telegram.
2. Команда `/newbot` → придумай имя и username.
3. BotFather пришлёт **токен** вида `7123456789:AAH...` — сохрани его.

**Узнать свой Telegram ID** (для админки):

- Напиши [@userinfobot](https://t.me/userinfobot) или [@getmyid_bot](https://t.me/getmyid_bot).
- Скопируй число (например `123456789`).

---

## Шаг 2. Google Cloud — JSON ключ

### 2.1. Проект и API

1. Открой [Google Cloud Console](https://console.cloud.google.com/).
2. **Select a project** → **New Project** → имя, например `baza-bot`.
3. Меню ☰ → **APIs & Services** → **Library**.
4. Найди **Google Sheets API** → **Enable**.

### 2.2. Service Account (сервисный аккаунт)

1. **APIs & Services** → **Credentials**.
2. **Create Credentials** → **Service account**.
3. Имя: `baza-sheets-bot` → **Create and Continue** → **Done**.
4. В списке кликни на созданный аккаунт → вкладка **Keys**.
5. **Add Key** → **Create new key** → **JSON** → **Create**.
6. Скачается файл `.json`.

### 2.3. Положить ключ в проект

```powershell
mkdir d:\baza\credentials
# Переименуй скачанный файл и положи сюда:
# d:\baza\credentials\google-service-account.json
```

Файл **не пустой**, внутри должны быть поля `client_email`, `private_key`, `project_id`.

Открой JSON и скопируй **`client_email`** — он выглядит так:
`baza-sheets-bot@baza-bot-123456.iam.gserviceaccount.com`

---

## Шаг 3. Google Таблица

### 3.1. Создать таблицу

1. [Google Drive](https://drive.google.com) → **Создать** → **Google Таблица**.
2. Переименуй файл, например «База поставщиков».

### 3.2. Дать доступ сервисному аккаунту

1. Кнопка **Поделиться** (Share).
2. Вставь `client_email` из JSON.
3. Роль: **Редактор** (Editor).
4. Сними галочку «Notify people» → **Share**.

### 3.3. ID таблицы

Из URL браузера:

```
https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz/edit
                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                      это GOOGLE_SPREADSHEET_ID
```

### 3.4. Структура листов (бот создаст сам, но можно проверить)

**Лист «Поставщики»** — шапка:

| id | город | что_поставляет | телефон | имя | telegram_user_id | обновлено |

**Лист «Заказчики»** — шапка:

| id | город | что_нужно | телефон | имя | telegram_user_id | обновлено |

---

## Шаг 4. Файл `.env`

```powershell
cd d:\baza
copy .env.example .env
```

Отредактируй `d:\baza\.env`:

```env
BOT_TOKEN=7123456789:AAHxxxxxxxxxxxxxxxxxxxxxxxxxxxx
ADMIN_IDS=123456789
DB_PATH=data/data.db
GOOGLE_SPREADSHEET_ID=1AbCdEfGhIjKlMnOpQrStUvWxYz
GOOGLE_CREDENTIALS_JSON=credentials/google-service-account.json
```

---

## Шаг 5. Установка и проверка

```powershell
cd d:\baza
pip install -r requirements.txt
python scripts/check_setup.py
```

Должно быть три `[OK]`:
- `.env`
- `Google Sheets`
- `Telegram bot`

Если `[FAIL] Google Sheets` — почти всегда:
- JSON пустой/битый
- таблица не расшарена на `client_email`
- неверный ID таблицы

---

## Шаг 6. Залить поставщиков из прайса

Если есть файл `Поставщики общий список.xlsx`:

```powershell
python scripts/push_to_google.py --in "Поставщики общий список.xlsx"
```

Или сначала конвертировать в локальный xlsx:

```powershell
python scripts/export_normalized.py --in "Поставщики общий список.xlsx"
# результат: data/google_suppliers.xlsx
```

Потом скопировать лист **Поставщики** в Google вручную (если без API-заливки).

---

## Шаг 7. Запуск бота

```powershell
python main.py
```

В логах:

```
Google Sheets connected: 'База поставщиков', лист «Поставщики»: 1420 строк
Bot started: @your_bot
Start polling...
```

**Docker:**

```powershell
docker compose up -d --build
docker compose logs -f
```

---

## Как пользоваться ботом

### Поставщик

1. `/start` → номер телефона
2. Город (из базы РФ/КЗ/БР)
3. **Поставщик** → «Что вы поставляете?»
4. Строка появляется/обновляется в Google → **Поставщики**

### Заказчик

1. `/start` → номер → город
2. **Заказчик** → «Что вам нужно?»
3. Бот ищет в Google → показывает поставщиков
4. Запись в Google → **Заказчики**

### Админ (твой Telegram ID в `ADMIN_IDS`)

- Кнопка **Админка** или `/admin`
- Выгрузка CSV из Google
- Рассылка пользователям бота
- **Свести** — уведомления при совпадении запроса заказчика и товара поставщика

---

## Чеклист проблем

| Симптом | Решение |
|---------|---------|
| `credentials file is not valid JSON` | JSON пустой — скачай ключ заново из Google Cloud |
| `Spreadsheet not found` | Неверный ID или нет доступа у service account |
| `403 Permission denied` | Не расшарил таблицу на `client_email` |
| Бот стартует, но поиск пустой | Лист **Поставщики** пустой — залей данные |
| `BOT_TOKEN is not set` | Создай `.env` из `.env.example` |

---

## Безопасность

- **Не коммить** `.env` и `credentials/` в git (уже в `.gitignore`).
- JSON ключ = полный доступ к таблице — храни локально.
- Токен бота — только в `.env`.
