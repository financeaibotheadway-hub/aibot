# -*- coding: utf-8 -*-
"""Very lightweight intent classification for Slack questions."""

from dataclasses import dataclass


@dataclass
class Intent:
    kind: str  # 'trend_root_cause', 'trend_compare', 'roas', 'generic'
    raw: str


def classify_intent(message: str) -> Intent:
    m = (message or "").lower()

    is_revenue = any(word in m for word in ["ревеню", "revenue", "дохід", "доход"])
    asks_why = any(word in m for word in ["чому", "причин", "причини"])
    asks_compare = any(word in m for word in ["порівняй", "порівняння", "порівняти"])

    if is_revenue and asks_why:
        return Intent(kind="trend_root_cause", raw=message)

    if is_revenue and asks_compare:
        return Intent(kind="trend_compare", raw=message)

    if "roas" in m:
        return Intent(kind="roas", raw=message)

    return Intent(kind="generic", raw=message)
