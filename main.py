from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import site
import sys
import time

from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.types import BufferedInputFile, CallbackQuery, Message, ReplyKeyboardMarkup, KeyboardButton, BotCommand
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import Config, load_config
from db import Database


PHONE_RE = re.compile(r"^(?:\+?7|8)\d{10}$")


def normalize_phone(raw: str) -> str | None:
    s = raw.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    digits = re.sub(r"\D", "", s)

    # Accept 10-digit local number (e.g. 9171234567) and convert to +7XXXXXXXXXX
    if len(digits) == 10:
        digits = "7" + digits

    if len(digits) != 11:
        return None

    # Convert 8XXXXXXXXXX to 7XXXXXXXXXX
    if digits.startswith("8"):
        digits = "7" + digits[1:]

    normalized = "+" + digits
    if not PHONE_RE.match(normalized):
        return None
    return normalized


def kb_role() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="Поставщик", callback_data="role:supplier")
    kb.button(text="Заказчик", callback_data="role:customer")
    kb.button(text="Поменять номер", callback_data="phone:change")
    kb.button(text="Поменять город", callback_data="city:change")
    kb.adjust(2)
    return kb


def kb_role_with_admin(is_admin_user: bool) -> InlineKeyboardBuilder:
    kb = kb_role()
    if is_admin_user:
        kb.button(text="Админка", callback_data="admin:panel")
        kb.adjust(2)
    return kb


async def kb_categories(db: Database) -> InlineKeyboardBuilder:
    cats = await db.list_enabled_categories()
    kb = InlineKeyboardBuilder()
    for c in cats:
        kb.button(text=c["name"], callback_data=f"cat:{c['id']}")
    kb.button(text="Назад", callback_data="nav:back_roles")
    kb.adjust(1)
    return kb


def kb_again() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="Записаться ещё раз", callback_data="again")
    kb.adjust(1)
    return kb


def kb_ok() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="OK", callback_data="ok:delete")
    kb.adjust(1)
    return kb


def kb_back_main() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="Назад", callback_data="nav:back_main")
    kb.adjust(1)
    return kb


def kb_admin() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text="Выгрузить поставщиков (CSV)", callback_data="admin:export:supplier")
    kb.button(text="Выгрузить заказчиков (CSV)", callback_data="admin:export:customer")
    kb.button(text="Рассылка", callback_data="admin:mail")
    kb.button(text="Свести (совпадения)", callback_data="admin:match")
    kb.button(text="Категории", callback_data="admin:cats")
    kb.button(text="Назад", callback_data="nav:back_roles")
    kb.adjust(1)
    return kb


async def kb_admin_cats(db: Database) -> InlineKeyboardBuilder:
    cats = await db.list_categories()
    kb = InlineKeyboardBuilder()
    for c in cats:
        suffix = "" if int(c["enabled"]) == 1 else " (выкл)"
        kb.button(
            text=f"{int(c['sort_order'])}. {c['name']}{suffix} [#{int(c['id'])}]",
            callback_data=f"admin:cat:{c['id']}",
        )
    kb.button(text="Добавить категорию", callback_data="admin:cat_add")
    kb.button(text="Назад", callback_data="admin:panel")
    kb.adjust(1)
    return kb


def is_admin(cfg: Config, user_id: int) -> bool:
    return user_id in cfg.admin_ids


class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, *, window_seconds: float = 0.3) -> None:
        self._window_seconds = window_seconds
        self._last_ts: dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user is None:
            return await handler(event, data)

        now = time.monotonic()
        prev = self._last_ts.get(user.id, 0.0)
        if now - prev < self._window_seconds:
            if isinstance(event, Message):
                try:
                    await event.delete()
                except Exception:
                    pass
            return

        self._last_ts[user.id] = now
        return await handler(event, data)


class RegistrationCleanupMiddleware(BaseMiddleware):
    def __init__(self, db: Database) -> None:
        self._db = db

    async def __call__(self, handler, event, data):
        if not isinstance(event, Message) or not event.from_user:
            return await handler(event, data)

        pending = await self._db.get_pending(event.from_user.id)
        result = await handler(event, data)

        if pending:
            try:
                await event.delete()
            except Exception:
                pass

        return result


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    cfg = load_config()
    db = Database(cfg.db_path)
    await db.init()

    def city_key(raw: str) -> str:
        s = (raw or "").strip().casefold()
        s = s.replace("ё", "е")
        # Replace any punctuation/separators with spaces (including apostrophes)
        s = re.sub(r"[^0-9a-zа-я]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def city_key_compact(raw: str) -> str:
        return city_key(raw).replace(" ", "")

    strict_cities: dict[str, str] = {}
    if cfg.cities:
        for display in cfg.cities.values():
            k = city_key(display)
            if k:
                strict_cities[k] = display
            kc = city_key_compact(display)
            if kc:
                strict_cities[kc] = display

    ru_cities: dict[str, str] = {}
    try:
        cities_path = os.path.join(os.path.dirname(__file__), "data", "cities_ru_kz_by.json")
        with open(cities_path, "r", encoding="utf-8") as f:
            cities = json.load(f)
        if not isinstance(cities, list):
            raise RuntimeError("cities_ru_kz_by.json must be a JSON list")
        for name in cities:
            if not isinstance(name, str):
                continue
            name = name.strip()
            if not name:
                continue
            k = city_key(name)
            if k and k not in ru_cities:
                ru_cities[k] = name
            kc = city_key_compact(name)
            if kc and kc not in ru_cities:
                ru_cities[kc] = name
    except Exception as e:
        logging.warning("Failed to load RU/KZ/BY cities dataset: %r", e)

    bot = Bot(cfg.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.message.outer_middleware(RegistrationCleanupMiddleware(db))
    dp.callback_query.outer_middleware(RateLimitMiddleware(window_seconds=0.15))

    await bot.set_my_commands([
        BotCommand(command="start", description="Запустить/Перезапустить бота"),
    ])

    me = await bot.get_me()
    logging.info("Bot started: @%s (id=%s)", me.username, me.id)

    async def ensure_bot_message(chat_id: int, user_id: int, text: str) -> int | None:
        pending = await db.get_pending(user_id)
        if pending and pending.bot_message_id:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=pending.bot_message_id, text=text)
                return pending.bot_message_id
            except Exception:
                pass
        try:
            sent = await bot.send_message(chat_id=chat_id, text=text)
            await db.upsert_pending(user_id, bot_message_id=sent.message_id)
            return sent.message_id
        except Exception as e:
            logging.warning("Failed to send/ensure bot message: %r", e)
            return None

    def normalize_city(raw: str) -> str | None:
        name = (raw or "").strip()
        if not name:
            return None

        keys: list[str] = []
        k1 = city_key(name)
        if k1:
            keys.append(k1)
        k1c = k1.replace(" ", "") if k1 else ""
        if k1c:
            keys.append(k1c)
        if not keys:
            return None

        # de-dup while keeping order
        seen = set()
        keys = [k for k in keys if not (k in seen or seen.add(k))]

        # 1. Check strict whitelist from .env if provided
        if cfg.cities:
            for k in keys:
                hit = strict_cities.get(k)
                if hit:
                    return hit
            return None

        # 2. Check against full RU cities database
        if ru_cities:
            for k in keys:
                hit = ru_cities.get(k)
                if hit:
                    return hit
            return None

        # No validation possible if both are empty (should not happen withRU dataset loaded)
        return None

    async def show_city_step(chat_id: int, user_id: int, *, show_back: bool = False) -> None:
        pending = await db.get_pending(user_id)
        msg_id = pending.bot_message_id if pending else None
        text = "Введите город:"
        markup = kb_back_main().as_markup() if show_back else None
        if msg_id:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=markup)
                return
            except Exception as e:
                logging.warning("Failed to edit message to city step: %r", e)

        try:
            sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
            await db.upsert_pending(user_id, bot_message_id=sent.message_id)
        except Exception as e:
            logging.warning("Failed to send city step: %r", e)

    async def send_main_role_message(chat_id: int, user_id: int, phone: str, city: str | None) -> None:
        city_line = f"Город: {city}\n" if city else ""
        text = f"Ваш номер: {phone}\n{city_line}\nВы поставщик или заказчик?"
        markup = kb_role_with_admin(is_admin(cfg, user_id)).as_markup()
        try:
            sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
            pending = await db.get_pending(user_id)
            await db.set_pending_exact(
                user_id,
                phone=phone,
                city=city,
                role=None,
                state=None,
                payload=None,
                bot_message_id=sent.message_id,
                menu_message_id=(pending.menu_message_id if pending else None),
            )
        except Exception as e:
            logging.warning("Failed to send main role message: %r", e)

    async def show_role_step(chat_id: int, user_id: int, phone: str, city: str | None) -> None:
        pending = await db.get_pending(user_id)
        msg_id = pending.bot_message_id if pending else None
        city_line = f"Город: {city}\n" if city else ""
        text = f"Ваш номер: {phone}\n{city_line}\nВы поставщик или заказчик?"
        markup = kb_role_with_admin(is_admin(cfg, user_id)).as_markup()
        if msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=text,
                    reply_markup=markup,
                )
                return
            except Exception as e:
                logging.warning("Failed to edit message to role step: %r", e)

        try:
            sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=markup)
            await db.upsert_pending(user_id, bot_message_id=sent.message_id)
        except Exception as e:
            logging.warning("Failed to send role step: %r", e)

    @dp.message(Command("start"))
    async def start(message: Message) -> None:
        reg_phone, reg_city = await db.get_registered_profile(message.from_user.id)
        pending = await db.get_pending(message.from_user.id)

        # Commands are set globally at startup, no need for hacky setup_menu_keyboard

        # While waiting for input (phone or product), /start must do nothing.
        if pending and (pending.phone is None or pending.state in {"await_city", "await_product", "await_phone_only"}):
            return

        phone = None
        city = None
        if pending and pending.phone:
            phone = pending.phone
            city = pending.city
        elif reg_phone:
            phone = reg_phone
            city = reg_city

        if phone:
            if not pending or not pending.bot_message_id:
                sent = await bot.send_message(chat_id=message.chat.id, text="Загрузка...")
                await db.upsert_pending(message.from_user.id, bot_message_id=sent.message_id)

            await db.upsert_pending(
                message.from_user.id,
                phone=phone,
                city=city,
                role=None,
                state=None,
                payload=None,
            )

            if city:
                await show_role_step(message.chat.id, message.from_user.id, phone, city)
            else:
                await db.upsert_pending(message.from_user.id, state="await_city")
                await show_city_step(message.chat.id, message.from_user.id)
            return

        msg_id = await ensure_bot_message(
            message.chat.id,
            message.from_user.id,
            "Отправьте ваш номер телефона (пример: +79991234567)",
        )
        await db.upsert_pending(
            message.from_user.id,
            phone=None,
            city=None,
            role=None,
            state=None,
            payload=None,
            bot_message_id=msg_id,
        )

    @dp.message(Command("admin"))
    async def admin(message: Message) -> None:
        if not is_admin(cfg, message.from_user.id):
            return
        pending = await db.get_pending(message.from_user.id)
        if pending and pending.bot_message_id:
            try:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=pending.bot_message_id,
                    text="Админ-панель:",
                    reply_markup=kb_admin().as_markup(),
                )
                return
            except Exception:
                pass
        msg_id = await ensure_bot_message(message.chat.id, message.from_user.id, "Админ-панель:")
        if msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=msg_id,
                    text="Админ-панель:",
                    reply_markup=kb_admin().as_markup(),
                )
            except Exception:
                pass
            await db.upsert_pending(message.from_user.id, bot_message_id=msg_id)

    @dp.message(F.text)
    async def any_text(message: Message) -> None:
        pending = await db.get_pending(message.from_user.id)
        if not pending:
            return

        if pending.phone is None:
            phone = normalize_phone(message.text or "")
            if not phone:
                try:
                    await message.delete()
                except Exception:
                    pass

                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=pending.bot_message_id,
                        text="Неправильный ввод. Попробуйте еще раз.\n\nОтправьте ваш номер телефона (пример: +79991234567)",
                        reply_markup=(kb_back_main().as_markup() if pending.state == "await_phone_only" or pending.payload else None),
                    )
                except Exception:
                    pass
                return

            # If user tapped "Change phone" we should NOT re-ask city.
            if pending.state == "await_phone_only":
                await db.set_pending_exact(
                    message.from_user.id,
                    phone=phone,
                    city=pending.city,
                    role=None,
                    state=None,
                    payload=None,
                    bot_message_id=pending.bot_message_id,
                    menu_message_id=pending.menu_message_id,
                )
                await show_role_step(message.chat.id, message.from_user.id, phone, pending.city)
                return

            await db.set_pending_exact(
                message.from_user.id,
                phone=phone,
                city=None,
                role=None,
                state="await_city",
                payload=None,
                bot_message_id=pending.bot_message_id,
                menu_message_id=pending.menu_message_id,
            )
            logging.info("Phone accepted for user_id=%s", message.from_user.id)
            await show_city_step(message.chat.id, message.from_user.id)
            return

        if pending.state == "await_city" and pending.phone and pending.bot_message_id:
            city = normalize_city(message.text)
            if not city:
                try:
                    await message.delete()
                except Exception:
                    pass
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=pending.bot_message_id,
                        text=f"Такого города нет в базе РФ/КЗ/БР ('{(message.text or '')[:20]}'). Попробуйте еще раз.\n\nВведите город:",
                        reply_markup=(kb_back_main().as_markup() if pending.payload else None),
                    )
                except Exception:
                    pass
                return

            await db.set_pending_exact(
                message.from_user.id,
                phone=pending.phone,
                city=city,
                role=None,
                state=None,
                payload=None,
                bot_message_id=pending.bot_message_id,
                menu_message_id=pending.menu_message_id,
            )
            await show_role_step(message.chat.id, message.from_user.id, pending.phone, city)
            return

        if pending.state == "await_product" and pending.phone and pending.role and pending.bot_message_id:
            product = (message.text or "").strip()
            if not product:
                return

            try:
                await message.delete()
            except Exception:
                pass

            await db.save_entry(
                user_id=message.from_user.id,
                role=pending.role,
                phone=pending.phone,
                city=pending.city,
                category=product,
            )
            await db.upsert_pending(message.from_user.id, state=None, payload=None, role=None)
            try:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=pending.bot_message_id,
                    text="Вы успешно записаны в базу, с вами скоро свяжутся.",
                    reply_markup=kb_ok().as_markup(),
                )
            except Exception:
                pass

            await send_main_role_message(message.chat.id, message.from_user.id, pending.phone, pending.city)
            return

        if pending.state == "admin_mail" and is_admin(cfg, message.from_user.id) and pending.bot_message_id:
            text = (message.text or "").strip()
            if not text:
                return

            try:
                await message.delete()
            except Exception:
                pass

            user_ids = await db.list_all_user_ids()
            ok = 0
            fail = 0
            for uid in user_ids:
                try:
                    await bot.send_message(uid, text)
                    ok += 1
                except Exception:
                    fail += 1

            await db.upsert_pending(message.from_user.id, state=None, payload=None)
            try:
                await bot.edit_message_text(
                    chat_id=message.chat.id,
                    message_id=pending.bot_message_id,
                    text=f"Рассылка произведена. Успешно: {ok}. Ошибок: {fail}.",
                    reply_markup=kb_admin().as_markup(),
                )
            except Exception:
                pass
            return

        if pending.state == "admin_rename" and pending.payload and is_admin(cfg, message.from_user.id):
            cat_id_str = pending.payload
            new_name = message.text.strip()
            if new_name:
                await db.rename_category(int(cat_id_str), new_name)
            try:
                await message.delete()
            except Exception:
                pass
            await db.upsert_pending(message.from_user.id, state=None, payload=None)
            refreshed = await kb_admin_cats(db)
            msg_id = pending.bot_message_id
            if not msg_id:
                msg_id = await ensure_bot_message(message.chat.id, message.from_user.id, "Категории:")
                await db.upsert_pending(message.from_user.id, bot_message_id=msg_id)

            if msg_id:
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=msg_id,
                        text="Категории:",
                        reply_markup=refreshed.as_markup(),
                    )
                    return
                except Exception:
                    pass
            return

        if pending.state == "admin_add" and is_admin(cfg, message.from_user.id):
            name = message.text.strip()
            if name:
                await db.add_category(name)
            try:
                await message.delete()
            except Exception:
                pass
            await db.delete_pending(message.from_user.id)
            refreshed = await kb_admin_cats(db)
            msg_id = pending.bot_message_id
            if not msg_id:
                msg_id = await ensure_bot_message(message.chat.id, message.from_user.id, "Категории:")
                await db.upsert_pending(message.from_user.id, bot_message_id=msg_id)

            if msg_id:
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=msg_id,
                        text="Категории:",
                        reply_markup=refreshed.as_markup(),
                    )
                    return
                except Exception:
                    pass
            return

    @dp.message(F.contact)
    async def any_contact(message: Message) -> None:
        pending = await db.get_pending(message.from_user.id)
        if not pending or pending.phone is not None:
            return
        if not message.contact or not message.contact.phone_number:
            try:
                await message.delete()
            except Exception:
                pass
            return
        phone = normalize_phone(message.contact.phone_number)
        if not phone:
            try:
                await message.delete()
            except Exception:
                pass
            return

        try:
            await message.delete()
        except Exception:
            pass

        await db.upsert_pending(message.from_user.id, phone=phone)
        logging.info("Phone accepted (contact) for user_id=%s", message.from_user.id)
        if pending.state == "await_phone_only":
            await db.set_pending_exact(
                message.from_user.id,
                phone=phone,
                city=pending.city,
                role=None,
                state=None,
                payload=None,
                bot_message_id=pending.bot_message_id,
                menu_message_id=pending.menu_message_id,
            )
            await show_role_step(message.chat.id, message.from_user.id, phone, pending.city)
        else:
            await db.set_pending_exact(
                message.from_user.id,
                phone=phone,
                city=None,
                role=None,
                state="await_city",
                payload=None,
                bot_message_id=pending.bot_message_id,
                menu_message_id=pending.menu_message_id,
            )
            await show_city_step(message.chat.id, message.from_user.id)

    @dp.callback_query(F.data == "phone:change")
    async def on_change_phone(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or not pending.bot_message_id:
            await cb.answer()
            return

        await db.set_pending_exact(
            cb.from_user.id,
            phone=None,
            city=pending.city,
            role=None,
            state="await_phone_only",
            payload=pending.phone,
            bot_message_id=pending.bot_message_id,
            menu_message_id=pending.menu_message_id,
        )
        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=pending.bot_message_id,
                text="Отправьте ваш номер телефона (пример: +79991234567)",
                reply_markup=kb_back_main().as_markup(),
            )
        except Exception as e:
            logging.warning("Failed to edit message to phone request: %r", e)
        await cb.answer()

    @dp.callback_query(F.data == "city:change")
    async def on_change_city(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or not pending.phone or not pending.bot_message_id:
            await cb.answer()
            return

        await db.set_pending_exact(
            cb.from_user.id,
            phone=pending.phone,
            city=None,
            role=None,
            state="await_city",
            payload=pending.city,
            bot_message_id=pending.bot_message_id,
            menu_message_id=pending.menu_message_id,
        )
        await show_city_step(cb.message.chat.id, cb.from_user.id, show_back=True)
        await cb.answer()

    @dp.callback_query(F.data == "ok:delete")
    async def on_ok_delete(cb: CallbackQuery) -> None:
        if cb.message:
            try:
                await cb.message.delete()
            except Exception:
                pass
        await cb.answer()

    @dp.callback_query(F.data == "nav:back_main")
    async def on_back_main(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or not pending.bot_message_id:
            await cb.answer()
            return

        # Restore previous values saved in payload.
        if pending.state == "await_phone_only" and pending.payload:
            restored_phone = pending.payload
            await db.set_pending_exact(
                cb.from_user.id,
                phone=restored_phone,
                city=pending.city,
                role=None,
                state=None,
                payload=None,
                bot_message_id=pending.bot_message_id,
                menu_message_id=pending.menu_message_id,
            )
            await show_role_step(cb.message.chat.id, cb.from_user.id, restored_phone, pending.city)
            await cb.answer()
            return

        if pending.state == "await_city" and pending.payload:
            restored_city = pending.payload
            await db.set_pending_exact(
                cb.from_user.id,
                phone=pending.phone,
                city=restored_city,
                role=None,
                state=None,
                payload=None,
                bot_message_id=pending.bot_message_id,
                menu_message_id=pending.menu_message_id,
            )
            await show_role_step(cb.message.chat.id, cb.from_user.id, pending.phone, restored_city)
            await cb.answer()
            return

        # Fallback: just show what we have.
        if pending.phone and pending.city:
            await show_role_step(cb.message.chat.id, cb.from_user.id, pending.phone, pending.city)
        await cb.answer()

    @dp.callback_query(F.data == "nav:back_roles")
    async def on_back_roles(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or not pending.phone:
            await cb.answer()
            return

        if not pending.city:
            await db.upsert_pending(cb.from_user.id, role=None, state="await_city", payload=None)
            await show_city_step(cb.message.chat.id, cb.from_user.id)
            await cb.answer()
            return

        await db.upsert_pending(cb.from_user.id, role=None, state=None, payload=None)
        await show_role_step(cb.message.chat.id, cb.from_user.id, pending.phone, pending.city)
        await cb.answer()

    @dp.callback_query(F.data.startswith("role:"))
    async def on_role(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or pending.phone is None or not pending.bot_message_id:
            await cb.answer()
            return

        role = cb.data.split(":", 1)[1]
        await db.upsert_pending(cb.from_user.id, role=role)

        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=pending.bot_message_id,
                text="Выберите категорию:",
                reply_markup=(await kb_categories(db)).as_markup(),
            )
        except Exception:
            pass
        await cb.answer()

    @dp.callback_query(F.data == "again")
    async def on_again(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or not pending.phone:
            await cb.answer()
            return
        await db.upsert_pending(cb.from_user.id, role=None, state=None, payload=None)

        if pending.city:
            await show_role_step(cb.message.chat.id, cb.from_user.id, pending.phone, pending.city)
        else:
            await db.upsert_pending(cb.from_user.id, state="await_city")
            await show_city_step(cb.message.chat.id, cb.from_user.id)
        await cb.answer()

    @dp.callback_query(F.data.startswith("cat:"))
    async def on_cat(cb: CallbackQuery) -> None:
        pending = await db.get_pending(cb.from_user.id)
        if not pending or not pending.phone or not pending.role or not pending.bot_message_id:
            await cb.answer()
            return

        cat_id = int(cb.data.split(":", 1)[1])
        cats = await db.list_categories()
        cat = next((c for c in cats if int(c["id"]) == cat_id), None)
        if not cat or int(cat["enabled"]) != 1:
            await cb.answer("Категория недоступна", show_alert=False)
            return

        if cat["name"].strip().lower() == "другое":
            await db.upsert_pending(cb.from_user.id, state="await_product", payload=None)
            try:
                await bot.edit_message_text(
                    chat_id=cb.message.chat.id,
                    message_id=pending.bot_message_id,
                    text="Введите продукт (нужный или поставляемый):",
                )
            except Exception:
                pass
            await cb.answer()
            return

        await db.save_entry(
            user_id=cb.from_user.id,
            role=pending.role,
            phone=pending.phone,
            city=pending.city,
            category=cat["name"],
        )
        await db.upsert_pending(cb.from_user.id, role=None, state=None, payload=None)

        try:
            await bot.edit_message_text(
                chat_id=cb.message.chat.id,
                message_id=pending.bot_message_id,
                text="Вы успешно записаны в базу, с вами скоро свяжутся.",
                reply_markup=kb_ok().as_markup(),
            )
        except Exception:
            pass

        await send_main_role_message(cb.message.chat.id, cb.from_user.id, pending.phone, pending.city)
        await cb.answer()

    @dp.callback_query(F.data.startswith("admin:"))
    async def on_admin(cb: CallbackQuery) -> None:
        if not is_admin(cfg, cb.from_user.id):
            await cb.answer()
            return

        if cb.message:
            await db.upsert_pending(cb.from_user.id, bot_message_id=cb.message.message_id)

        parts = cb.data.split(":")
        if parts[1] == "panel":
            pending = await db.get_pending(cb.from_user.id)
            if pending and pending.bot_message_id:
                try:
                    await bot.edit_message_text(
                        chat_id=cb.message.chat.id,
                        message_id=pending.bot_message_id,
                        text="Админ-панель:",
                        reply_markup=kb_admin().as_markup(),
                    )
                except Exception:
                    pass
            await cb.answer()
            return

        if parts[1] == "mail":
            pending = await db.get_pending(cb.from_user.id)
            if not pending or not pending.bot_message_id:
                await cb.answer()
                return
            await db.upsert_pending(cb.from_user.id, state="admin_mail", payload=None)
            try:
                await bot.edit_message_text(
                    chat_id=cb.message.chat.id,
                    message_id=pending.bot_message_id,
                    text="Введите сообщение для рассылки:",
                    reply_markup=None,
                )
            except Exception:
                pass
            await cb.answer()
            return

        if parts[1] == "match":
            pending = await db.get_pending(cb.from_user.id)
            if not pending or not pending.bot_message_id:
                await cb.answer()
                return
            matches = await db.find_matches()
            sent_pairs = set()
            notify_ok = 0
            for m in matches:
                key = (int(m["customer_user_id"]), int(m["supplier_user_id"]), str(m["norm_category"]))
                if key in sent_pairs:
                    continue
                sent_pairs.add(key)
                cat = m["customer_category"]
                try:
                    await bot.send_message(
                        int(m["customer_user_id"]),
                        f"Найден поставщик по запросу '{cat}'. Телефон поставщика: {m['supplier_phone']}",
                    )
                    await bot.send_message(
                        int(m["supplier_user_id"]),
                        f"Найден заказчик по товару '{cat}'. Телефон заказчика: {m['customer_phone']}",
                    )
                    notify_ok += 1
                except Exception:
                    pass

            try:
                await bot.edit_message_text(
                    chat_id=cb.message.chat.id,
                    message_id=pending.bot_message_id,
                    text=f"Совпадения обработаны: {notify_ok}.",
                    reply_markup=kb_admin().as_markup(),
                )
            except Exception:
                pass
            await cb.answer()
            return
        if parts[1] == "export":
            role = parts[2]
            rows = await db.export_rows(role)
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=["id", "user_id", "phone", "city", "category", "created_at"],
                delimiter=";",
            )
            writer.writeheader()
            writer.writerows(rows)
            data = output.getvalue().encode("utf-8-sig")
            buf = BufferedInputFile(data, filename=f"{role}s.csv")
            await cb.message.answer_document(buf)
            await cb.answer()
            return

        if parts[1] == "cats":
            await cb.message.edit_text("Категории:", reply_markup=(await kb_admin_cats(db)).as_markup())
            await cb.answer()
            return

        if parts[1] == "cat_add":
            pending = await db.get_pending(cb.from_user.id)
            if pending and pending.bot_message_id:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад", callback_data="admin:cats")
                kb.adjust(1)
                try:
                    await bot.edit_message_text(
                        chat_id=cb.message.chat.id,
                        message_id=pending.bot_message_id,
                        text="Отправьте название новой категории",
                        reply_markup=kb.as_markup(),
                    )
                except Exception:
                    pass
            await db.upsert_pending(cb.from_user.id, state="admin_add", payload=None)
            await cb.answer()
            return

        if parts[1] == "cat":
            cat_id = int(parts[2])
            cat = await db.get_category(cat_id)
            kb = InlineKeyboardBuilder()
            kb.button(text="Вверх", callback_data=f"admin:cat_move:{cat_id}:up")
            kb.button(text="Вниз", callback_data=f"admin:cat_move:{cat_id}:down")
            kb.button(text="Переименовать", callback_data=f"admin:cat_rename:{cat_id}")
            kb.button(text="Вкл/Выкл", callback_data=f"admin:cat_toggle:{cat_id}")
            kb.button(text="Удалить", callback_data=f"admin:cat_del:{cat_id}")
            kb.button(text="Назад", callback_data="admin:cats")
            kb.adjust(2)
            title = "Управление категорией"
            if cat:
                title = f"Управление категорией: {cat['name']} (порядок: {int(cat['sort_order'])}, id: {int(cat['id'])})"
            await cb.message.edit_text(title, reply_markup=kb.as_markup())
            await cb.answer()
            return

        if parts[1] == "cat_rename":
            cat_id = int(parts[2])
            await db.upsert_pending(cb.from_user.id, state="admin_rename", payload=str(cat_id))
            pending = await db.get_pending(cb.from_user.id)
            if pending and pending.bot_message_id:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад", callback_data=f"admin:cat:{cat_id}")
                kb.adjust(1)
                try:
                    await bot.edit_message_text(
                        chat_id=cb.message.chat.id,
                        message_id=pending.bot_message_id,
                        text="Отправьте новое название категории",
                        reply_markup=kb.as_markup(),
                    )
                except Exception:
                    pass
            await cb.answer()
            return

        if parts[1] == "cat_toggle":
            cat_id = int(parts[2])
            await db.toggle_category(cat_id)
            await cb.message.edit_text("Категории:", reply_markup=(await kb_admin_cats(db)).as_markup())
            await cb.answer("Готово")
            return

        if parts[1] == "cat_del":
            cat_id = int(parts[2])
            await db.delete_category(cat_id)
            await cb.message.edit_text("Категории:", reply_markup=(await kb_admin_cats(db)).as_markup())
            await cb.answer("Удалено")
            return

        if parts[1] == "cat_move":
            cat_id = int(parts[2])
            direction = parts[3]
            ok = await db.move_category(cat_id, direction)
            if ok:
                await cb.message.edit_text("Категории:", reply_markup=(await kb_admin_cats(db)).as_markup())
                await cb.answer("Готово")
            else:
                await cb.answer("Нельзя переместить", show_alert=False)
            return

        await cb.answer()

    @dp.error()
    async def on_error(event, exception: Exception):
        # do not crash polling loop
        try:
            logging.exception("Unhandled error: %r", exception)
        except Exception:
            pass
        return True

    try:
        logging.info("Start polling...")
        await dp.start_polling(bot)
    finally:
        logging.info("Polling stopped")


if __name__ == "__main__":
    asyncio.run(main())
