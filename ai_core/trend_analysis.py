# -*- coding: utf-8 -*-
"""Fixed‑SQL pipeline for revenue trend questions."""

import calendar
import datetime as dt
import logging
from typing import Dict, Any

import pandas as pd

from .config import LOCAL_TZ, BQ_DATASET, BQ_PROJECT, BQ_REVENUE_TABLE
from .db import execute_query
from .period_parser import parse_comparison_periods, ComparisonSpec
from .vertex_client import generate_text

logger = logging.getLogger("ai-bot")

REVENUE_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_REVENUE_TABLE}"


def _month_range(year: int, month: int):
    start = dt.date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = dt.date(year, month, last_day)
    return start, end


def _build_year_ranges(spec: ComparisonSpec):
    cur_y = spec.current_year.year
    prev_y = spec.prev_year.year
    cur_start = dt.date(cur_y, 1, 1)
    cur_end = dt.date(cur_y, 12, 31)
    prev_start = dt.date(prev_y, 1, 1)
    prev_end = dt.date(prev_y, 12, 31)
    return (prev_start, prev_end, cur_start, cur_end)


def _fetch_revenue_summary(start: dt.date, end: dt.date) -> pd.DataFrame:
    sql = f"""    SELECT
      DATE(posting_date) AS date,
      SUM(gross_usd) AS revenue
    FROM `{REVENUE_TABLE_REF}`
    WHERE posting_date BETWEEN DATE('{start}') AND DATE('{end}')
    GROUP BY date
    ORDER BY date
    """
    return execute_query(sql)


def _fetch_breakdown(start: dt.date, end: dt.date, dimension: str) -> pd.DataFrame:
    if dimension not in {"product_id", "channel", "country"}:
        raise ValueError("Unsupported dimension")
    sql = f"""    SELECT
      {dimension},
      SUM(gross_usd) AS revenue
    FROM `{REVENUE_TABLE_REF}`
    WHERE posting_date BETWEEN DATE('{start}') AND DATE('{end}')
    GROUP BY {dimension}
    HAVING revenue > 0
    ORDER BY revenue DESC
    LIMIT 50
    """
    return execute_query(sql)


def _prepare_ai_payload(spec: ComparisonSpec) -> Dict[str, Any]:
    if spec.granularity == "month":
        ps, pe = _month_range(spec.prev_month.year, spec.prev_month.month)
        cs, ce = _month_range(spec.current_month.year, spec.current_month.month)
    else:
        ps, pe, cs, ce = _build_year_ranges(spec)

    prev_df = _fetch_revenue_summary(ps, pe)
    cur_df = _fetch_revenue_summary(cs, ce)

    # basic totals
    prev_total = float(prev_df["revenue"].sum()) if not prev_df.empty else 0.0
    cur_total = float(cur_df["revenue"].sum()) if not cur_df.empty else 0.0

    # breakdowns by product and channel (if present in schema)
    try:
        prev_prod = _fetch_breakdown(ps, pe, "product_id")
        cur_prod = _fetch_breakdown(cs, ce, "product_id")
    except Exception:
        prev_prod = cur_prod = pd.DataFrame()

    try:
        prev_ch = _fetch_breakdown(ps, pe, "channel")
        cur_ch = _fetch_breakdown(cs, ce, "channel")
    except Exception:
        prev_ch = cur_ch = pd.DataFrame()

    def _df_to_small_csv(df: pd.DataFrame, max_rows: int = 20) -> str:
        if df.empty:
            return "EMPTY"
        return df.head(max_rows).to_csv(index=False)

    payload = {
        "granularity": spec.granularity,
        "prev_period": {"start": str(ps), "end": str(pe), "total_revenue": prev_total},
        "current_period": {"start": str(cs), "end": str(ce), "total_revenue": cur_total},
        "prev_daily": _df_to_small_csv(prev_df),
        "current_daily": _df_to_small_csv(cur_df),
        "prev_by_product": _df_to_small_csv(prev_prod),
        "current_by_product": _df_to_small_csv(cur_prod),
        "prev_by_channel": _df_to_small_csv(prev_ch),
        "current_by_channel": _df_to_small_csv(cur_ch),
    }
    return payload


def answer_trend_question(question: str, intent_kind: str) -> str:
    """Main entry: build fixed SQL, then ask AI to explain the deltas."""
    spec = parse_comparison_periods(question)
    if not spec:
        return (
            "Не зміг зрозуміти, які саме періоди треба порівняти. "
            "Сформулюй, будь ласка, запит у форматі: "
            ""Які причини падіння ревеню у травні 2024 року відносно квітня 2024?" "
            "або "Назви причини падіння ревеню у 2025 році порівняно з 2024 роком"."
        )

    payload = _prepare_ai_payload(spec)

    prompt = f"""Ти — аналітик даних. У тебе є агреговані дані по ревеню за два періоди.

Питання користувача (українською):
"""{question}"""


JSON з підготовленими даними:
```json
{payload}
```

Твоя задача:
- Поясни, чи зріс чи впав ревеню в поточному періоді проти попереднього.
- Наведи 2–4 ключові драйвери (категорії / продукти / канали), які дали найбільший вклад у зміну.
- Якщо різниця невелика — так і скажи.
- Не вигадуй цифри, яких немає в даних. Говори якісно: "значне падіння", "помірне зростання" тощо.
- Відповідай стисло, 4–6 речень українською, без SQL.
"""

    answer = generate_text(prompt, temperature=0.2)
    return answer
