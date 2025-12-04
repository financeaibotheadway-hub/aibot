# -*- coding: utf-8 -*-
"""Fallback path: AI generates SQL, we lightly post‑process and execute it."""

import json
import logging
import re
from typing import Set

import pandas as pd
from google.api_core.exceptions import BadRequest

from .config import (
    BQ_PROJECT,
    BQ_DATASET,
    BQ_REVENUE_TABLE,
    BQ_COST_TABLE,
    LOCAL_TZ,
    RETURN_SQL_ON_ERROR,
)
from .db import execute_query
from .vertex_client import generate_text
from . import schema_utils

logger = logging.getLogger("ai-bot")

REVENUE_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_REVENUE_TABLE}"
COST_TABLE_REF    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_COST_TABLE}"


def _limited_csv(df: pd.DataFrame, max_rows: int = 7) -> str:
    if df.empty:
        return "EMPTY_RESULT"
    if len(df) <= max_rows:
        return df.to_csv(index=False)
    half = max_rows // 2
    head = df.head(half)
    tail = df.tail(max_rows - half)
    txt = "HEAD:\n" + head.to_csv(index=False)
    txt += "\nTAIL:\n" + tail.to_csv(index=False)
    txt += f"\n[TRUNCATED] total_rows={len(df)}"
    return txt


def _sanitize_sql_dates(sql_query: str, date_columns: Set[str]) -> str:
    original = sql_query
    upper = sql_query.upper()

    if "CURRENT_DATE" not in upper and "PARSE_DATE" not in upper and "SAFE.PARSE_DATE" not in upper:
        return sql_query

    sql_query = re.sub(
        r"\bCURRENT_DATE\s*\(\s*\)",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )
    sql_query = re.sub(
        r"\bCURRENT_DATE\b(?!\s*\()",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )

    for col in sorted(date_columns, key=len, reverse=True):
        pattern_plain = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"

        def _repl_plain(m):
            inner = m.group(1)
            inner_clean = inner.strip('`')
            if inner_clean.endswith(f".{col}") or inner_clean == col:
                return inner
            return m.group(0)

        sql_query = re.sub(pattern_plain, _repl_plain, sql_query, flags=re.IGNORECASE)

        pattern_safe = rf"SAFE\.PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"

        def _repl_safe(m):
            inner = m.group(1)
            inner_clean = inner.strip('`')
            if inner_clean.endswith(f".{col}") or inner_clean == col:
                return f"CAST({inner} AS DATE)"
            return m.group(0)

        sql_query = re.sub(pattern_safe, _repl_safe, sql_query, flags=re.IGNORECASE)

    if sql_query != original:
        logger.info("[sanitize] SQL was sanitized for date handling")

    return sql_query


def _fix_format_date(sql: str) -> str:
    sql = re.sub(
        r"FORMAT_DATE\s*\(\s*'([^']+)'\s*\)",
        r"FORMAT_DATE('\1', posting_date)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"FORMAT_DATE\s*\(([^,]+),\s*([^\)]+),\s*\)",
        r"FORMAT_DATE(\1, \2)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"FORMAT_DATE\s*\(\s*'([^']+)'\s+(\w+)\s*\)",
        r"FORMAT_DATE('\1', \2)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(FORMAT_DATE\s*\([^\)]+\))(?!\s+as|\s*,)",
        r"\1 AS month",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _fix_trailing_commas(sql: str) -> str:
    sql = re.sub(r",\s*(FROM|WHERE|GROUP BY|ORDER BY)", r" \1", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\n\s*(FROM|WHERE|GROUP BY|ORDER BY)", r"\n\1", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*,", ", ", sql)
    sql = re.sub(r",\s*\)", ")", sql)
    return sql


def _auto_fix_group_by(sql: str) -> str:
    try:
        m = re.search(r"select(.*?)from", sql, re.IGNORECASE | re.DOTALL)
        if not m:
            return sql
        select_block = m.group(1)

        fields = []
        for part in select_block.split(","):
            clean = part.strip()
            if not clean:
                continue
            if re.search(r"(sum|count|min|max|avg)\s*\(", clean, re.IGNORECASE):
                continue
            clean_no_alias = re.sub(r"\s+as\s+.*", "", clean, flags=re.IGNORECASE).strip()
            if clean_no_alias.startswith("'") or clean_no_alias.startswith('"'):
                continue
            fields.append(clean_no_alias)

        gb = re.search(
            r"group\s+by(.*?)(order\s+by|limit|$)",
            sql,
            re.IGNORECASE | re.DOTALL,
        )
        if not gb:
            return sql

        group_by_raw = gb.group(1)
        group_cols = [x.strip() for x in group_by_raw.split(",") if x.strip()]

        missing = []
        for f in fields:
            base_f = f.split(".")[-1].lower()
            found = any(base_f == g.split(".")[-1].lower() for g in group_cols)
            if not found:
                missing.append(f)

        if not missing:
            return sql

        new_group_by = "GROUP BY " + ", ".join(group_cols + missing)

        fixed_sql = re.sub(
            r"group\s+by(.*?)(order\s+by|limit|$)",
            new_group_by + r" \2",
            sql,
            flags=re.IGNORECASE | re.DOTALL,
        )
        logger.info("[auto_fix_group_by] added to GROUP BY: %s", missing)
        return fixed_sql
    except Exception:
        logger.exception("[auto_fix_group_by] failed")
        return sql


def _full_fix(sql: str, date_cols: set) -> str:
    before = sql
    sql = _sanitize_sql_dates(sql, date_cols)
    sql = _fix_format_date(sql)
    sql = _fix_trailing_commas(sql)
    sql = _auto_fix_group_by(sql)
    if sql != before:
        logger.info("[sql fix] SQL was post-processed by _full_fix")
    return sql


def run_generic_sql(instruction: str) -> str:
    rev_schema, cost_schema = schema_utils.get_all_schemas(REVENUE_TABLE_REF, COST_TABLE_REF)
    date_cols = schema_utils.collect_date_columns(rev_schema) | schema_utils.collect_date_columns(cost_schema)
    date_cols_list = sorted(list(date_cols))

    sql_prompt = f"""\
В нас є ДВІ таблиці в BigQuery:

1) REVENUE: `{REVENUE_TABLE_REF}`
Схема:
{json.dumps(rev_schema, indent=2, ensure_ascii=False)}

2) COST: `{COST_TABLE_REF}`
Схема:
{json.dumps(cost_schema, indent=2, ensure_ascii=False)}

Згенеруй BigQuery SQL для завдання: {instruction}

Правила:
- Використовуй ТІЛЬКИ BigQuery SQL.
- Не використовуй STRFTIME; для форматів дат: FORMAT_DATE('%Y-%m', DATE(...)).
- Не використовуй корельовані підзапити.
- Якщо запит тільки про дохід/продажі — REVENUE.
- Якщо тільки про витрати — COST.
- Для ROAS/прибутку — агрегуй окремо та JOIN.
- У REVENUE для «net revenue» — сумуй gross_usd (усі event_type).
- period (12M/1M/6M) — це тип підписки, не час.
- Часовий пояс для відносних дат: CURRENT_DATE('{LOCAL_TZ}').
- Дейт-поля не парсити як STRING. Вони вже типу DATE/DATETIME/TIMESTAMP: {date_cols_list}.
- Поверни лише фінальний SQL без пояснень.
"""

    sql_query = generate_text(sql_prompt, temperature=0)
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    if sql_query.lower().startswith("sql"):
        sql_query = sql_query[3:].strip()

    logger.info("[generic_sql] raw: %s", sql_query)

    sql_query = _full_fix(sql_query, date_cols)
    logger.info("[generic_sql] fixed: %s", sql_query)

    try:
        df = execute_query(sql_query)
    except BadRequest as e:
        msg = getattr(e, "message", str(e))[:600]
        out = "❌ **Помилка при виконанні запиту до бази даних.**\n"
        if RETURN_SQL_ON_ERROR:
            out += f"SQL:\n```sql\n{sql_query[:1500]}\n```\n"
        out += f"Помилка BigQuery:\n```\n{msg}\n```"
        return out
    except Exception as e:
        msg = (getattr(e, "message", None) or str(e))[:600]
        out = "❌ **Помилка при виконанні запиту до бази даних.**\n"
        if RETURN_SQL_ON_ERROR:
            out += f"SQL:\n```sql\n{sql_query[:1500]}\n```\n"
        out += f"Деталі:\n```\n{msg}\n```"
        return out

    if df.empty:
        return "Результат таблиці порожній."

    analysis_prompt = f"""\
Зроби те, що просить користувач в інструкції.
Інструкція: "{instruction}"

CSV результат SQL (урізаний до важливого):
{_limited_csv(df)}

Вимоги:
- Не повертай SQL у відповіді.
- Не вигадуй даних або дат — тільки те, що в таблиці.
- Якщо просили аналіз — до 3–4 речень.
- period (12M/1M/6M) — це типи підписок, не час.
"""

    answer = generate_text(analysis_prompt, temperature=0)
    return answer.strip()
