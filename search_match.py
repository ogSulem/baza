"""Поиск поставщиков по тексту запроса заказчика."""

from __future__ import annotations

import re


def _tokens(text: str) -> list[str]:
    t = (text or "").strip().casefold().replace("ё", "е")
    parts = [p for p in re.split(r"[^0-9a-zа-я]+", t) if p]
    long = [p for p in parts if len(p) >= 3]
    return long if long else [p for p in parts if len(p) >= 2]


def supply_matches(query: str, supply: str) -> bool:
    q = (query or "").strip().casefold().replace("ё", "е")
    s = (supply or "").strip().casefold().replace("ё", "е")
    if not q or not s:
        return False
    if q in s or s in q:
        return True
    tokens = _tokens(q)
    if not tokens:
        return False
    # Check if any token from query matches in supply
    if any(tok in s for tok in tokens):
        return True
    # Also check tokens from supply against query (more flexible matching)
    supply_tokens = _tokens(s)
    if any(stok in q for stok in supply_tokens):
        return True
    return False
