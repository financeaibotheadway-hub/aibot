# -*- coding: utf-8 -*-
"""Heuristic parser for Ukrainian time periods in questions.

Covers phrases like:
- "у травні 2024 року відносно квітня 2024"
- "у 2025 році порівняно з 2024 роком"
"""

import re
from dataclasses import dataclass
from typing import Optional


MONTH_STEMS = {
    "січ": 1,
    "лют": 2,
    "берез": 3,
    "квіт": 4,
    "трав": 5,
    "черв": 6,
    "лип": 7,
    "серп": 8,
    "верес": 9,
    "жовт": 10,
    "листоп": 11,
    "груд": 12,
}


@dataclass
class PeriodMonth:
    year: int
    month: int


@dataclass
class PeriodYear:
    year: int


@dataclass
class ComparisonSpec:
    granularity: str  # 'month' or 'year'
    current_month: Optional[PeriodMonth] = None
    prev_month: Optional[PeriodMonth] = None
    current_year: Optional[PeriodYear] = None
    prev_year: Optional[PeriodYear] = None


def _find_years(text: str):
    return [int(y) for y in re.findall(r"(20\d{2})", text)]


def _find_months(text: str):
    text_l = text.lower()
    found = []
    for stem, num in MONTH_STEMS.items():
        idx = text_l.find(stem)
        if idx >= 0:
            found.append((idx, num, stem))
    found.sort(key=lambda x: x[0])
    return found


def parse_comparison_periods(message: str) -> Optional[ComparisonSpec]:
    """Best‑effort extraction of periods from a Ukrainian revenue question."""
    if not message:
        return None
    text = message.lower()

    years = _find_years(text)
    months = _find_months(text)

    # Month‑to‑month within same year
    if len(months) >= 2 and years:
        cur_m = months[0][1]
        prev_m = months[1][1]
        year = years[0]
        return ComparisonSpec(
            granularity="month",
            current_month=PeriodMonth(year=year, month=cur_m),
            prev_month=PeriodMonth(year=year, month=prev_m),
        )

    # Pure year‑to‑year
    if len(years) >= 2:
        cur_y = years[0]
        prev_y = years[1]
        return ComparisonSpec(
            granularity="year",
            current_year=PeriodYear(year=cur_y),
            prev_year=PeriodYear(year=prev_y),
        )

    return None
