from __future__ import annotations

import re

PHONE_RE = re.compile(r"\b(?:\+?7|8)?\d{10}\b")
URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)


def normalize_phone(raw: str) -> str | None:
    s = (raw or "").strip()
    s = re.sub(r"[^0-9+]", "", s)
    digits = re.sub(r"\D", "", s)

    if len(digits) == 10:
        digits = "7" + digits
    if len(digits) != 11:
        return None
    if digits.startswith("8"):
        digits = "7" + digits[1:]
    return "+" + digits


def contact_phone_field(*, phone: str | None, url: str | None) -> str:
    if phone:
        return phone
    if url:
        return url.strip()
    return ""
