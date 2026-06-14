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
    """В колонку «телефон»: номер, иначе ссылка Avito."""
    if phone:
        return phone
    if url:
        return url.strip()
    return ""


def parse_contacts_line(line: str) -> tuple[str | None, str | None, str | None]:
    line = (line or "").strip()
    if not line:
        return None, None, None

    url = None
    m_url = URL_RE.search(line)
    if m_url:
        url = m_url.group(0).strip()

    phone = None
    m_phone = PHONE_RE.search(line)
    if m_phone:
        phone = normalize_phone(m_phone.group(0))

    name = None
    parts = [p.strip() for p in re.split(r"\t+", line) if p.strip()]
    if len(parts) >= 2:
        url_part = None
        phone_part = None
        for p in parts:
            if url_part is None and URL_RE.search(p):
                url_part = p
            if phone_part is None and PHONE_RE.search(p):
                phone_part = p

        if url_part and not url:
            m = URL_RE.search(url_part)
            url = m.group(0).strip() if m else url
        if phone_part and not phone:
            m = PHONE_RE.search(phone_part)
            phone = normalize_phone(m.group(0)) if m else phone

        for p in parts:
            if url_part and p == url_part:
                continue
            if phone_part and p == phone_part:
                continue
            name = p
            break
        if not name:
            name = parts[-1]
    else:
        if url:
            rest = line.replace(url, " ").strip()
            rest = re.sub(r"\s+", " ", rest)
            if rest:
                name = rest
        elif phone and m_phone:
            rest = line.replace(m_phone.group(0), " ").strip()
            rest = re.sub(r"\s+", " ", rest)
            if rest:
                name = rest

    return phone, name, url
