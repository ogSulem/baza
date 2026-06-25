"""Поиск поставщиков по тексту запроса заказчика."""

from __future__ import annotations

import re


def _lemmatize(word: str) -> str:
    """Простая лемматизация для русского языка - удаление окончаний"""
    word = word.casefold()
    # Русские окончания
    endings = ["ый", "ий", "ой", "ая", "яя", "ое", "ее", "ые", "ие", "ых", "их",
               "ому", "ему", "ого", "его", "ой", "ей", "ую", "юю", "ым", "им",
               "ам", "ям", "ами", "ями", "ах", "ях", "а", "я", "о", "е", "ы", "и",
               "у", "ю", "ов", "ев", "ь", "й"]
    for ending in endings:
        if word.endswith(ending) and len(word) > len(ending) + 2:
            return word[:-len(ending)]
    return word

def _tokens(text: str) -> list[str]:
    t = (text or "").strip().casefold().replace("ё", "е")
    parts = [p for p in re.split(r"[^0-9a-zа-я]+", t) if p]
    # Лемматизируем каждое слово
    lemmatized = [_lemmatize(p) for p in parts]
    long = [p for p in lemmatized if len(p) >= 3]
    return long if long else [p for p in lemmatized if len(p) >= 2]


def _similarity(s1: str, s2: str) -> float:
    """Вычисляет процент схожести двух строк (простой алгоритм)"""
    if not s1 or not s2:
        return 0.0
    s1 = s1.casefold()
    s2 = s2.casefold()
    # Levenshtein distance упрощённый
    if len(s1) < len(s2):
        s1, s2 = s2, s1
    if len(s2) == 0:
        return 0.0
    # Простое сравнение по символам
    matches = sum(c1 == c2 for c1, c2 in zip(s1, s2))
    return matches / max(len(s1), len(s2))

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
    # Нечёткий поиск - если схожесть > 60%
    if _similarity(q, s) > 0.6:
        return True
    # Проверяем схожесть токенов
    for tok in tokens:
        for stok in supply_tokens:
            if _similarity(tok, stok) > 0.7:
                return True
    return False
