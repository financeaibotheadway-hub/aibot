# analytics/analytics_core.py
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import hashlib
import logging
import traceback
from functools import lru_cache

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, GoogleAPIError

import vertexai
from vertexai.preview.generative_models import GenerativeModel

from semantic_map import semantic_map

# >>>>>>>>>>>> INTEGRATION (NEW)
from analytics.metric_loader import get_metrics
from analytics.metric_parser import detect_metric
from analytics.trend_analysis import run_trend_analysis
# <<<<<<<<<<<< INTEGRATION END


# ──────────────────────────────────────────────────────────────────────────────
# ENV / LOGGING
# ──────────────────────────────────────────────────────────────────────────────
BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")
VERTEX_LOCATION  = os.getenv("VERTEX_LOCATION", "europe-west1")
LOCAL_TZ         = os.getenv("LOCAL_TZ", "Europe/Kyiv")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("ai-bot")

RETURN_SQL_ON_ERROR = os.getenv("RETURN_SQL_ON_ERROR", "false").lower() == "true"


# ──────────────────────────────────────────────────────────────────────────────
# INIT CLIENTS
# ──────────────────────────────────────────────────────────────────────────────
REVENUE_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_REVENUE_TABLE}"
COST_TABLE_REF    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_COST_TABLE}"

bq_client = bigquery.Client(project=BQ_PROJECT)

try:
    vertexai.init(project=BQ_PROJECT, location=VERTEX_LOCATION)
except Exception:
    logger.warning("Vertex init failed", exc_info=True)

model = GenerativeModel("gemini-2.5-flash")

query_cache = {}
cache_ttl = 300

_schema_cache = {}
_schema_time  = {}


def get_cache_key(query: str) -> str:
    return hashlib.md5(query.encode("utf-8")).hexdigest()


def get_table_schema(table_ref: str, ttl_sec: int = 3600):
    now = time.time()
    if table_ref not in _schema_cache or now - _schema_time.get(table_ref, 0) > ttl_sec:
        schema = bq_client.get_table(table_ref).schema
        _schema_cache[table_ref] = [{"name": c.name, "type": c.field_type} for c in schema]
        _schema_time[table_ref] = now
    return _schema_cache[table_ref]


def get_all_schemas():
    rev_schema = get_table_schema(REVENUE_TABLE_REF)
    try:
        cost_schema = get_table_schema(COST_TABLE_REF)
    except Exception:
        cost_schema = []
    return rev_schema, cost_schema


# >>> preload
_ = get_all_schemas()

# ──────────────────────────────────────────────────────────────────────────────
# LLM SQL SANITIZER
# ──────────────────────────────────────────────────────────────────────────────
def sanitize_llm_sql(sql: str) -> str:
    sql = sql.strip()

    # remove markdown fences
    sql = re.sub(r"```(sql|bigquery)?", "", sql, flags=re.IGNORECASE)
    sql = sql.replace("```", "")

    # remove accidental 'bigquery' token at start
    sql = re.sub(r"(?i)^\s*bigquery\s*", "", sql)

    return sql.strip()

# ──────────────────────────────────────────────────────────────────────────────
# DATE TOOLS
# ──────────────────────────────────────────────────────────────────────────────
def _collect_date_columns(schema_list):
    return {
        f["name"]
        for f in schema_list
        if f.get("type") in ("DATE", "DATETIME", "TIMESTAMP")
    }


def _sanitize_sql_dates(sql_query: str, date_columns: set) -> str:
    """
    Normalize dates for BigQuery:
    - CURRENT_DATE / CURRENT_DATE() → CURRENT_DATE('TZ')
    - Remove PARSE_DATE around real DATE columns
    - Fix placeholder dates like 'YYYY-MM-01'
    - Wrap naked 'YYYY-MM-DD' literals into DATE(...)
    """

    # CURRENT_DATE()
    sql_query = re.sub(
        r"\bCURRENT_DATE\s*\(\s*\)",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )

    # CURRENT_DATE without ()
    sql_query = re.sub(
        r"\bCURRENT_DATE\b(?!\s*\()",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )

    # Remove PARSE_DATE around DATE columns
    for col in date_columns:
        pattern = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"

        def _unwrap_parse_date(m):
            inner = m.group(1)
            clean = inner.strip("`")
            if clean == col or clean.endswith(f".{col}"):
                return inner
            return m.group(0)

        sql_query = re.sub(pattern, _unwrap_parse_date, sql_query, flags=re.IGNORECASE)

    # ✅ FIX placeholder 'YYYY-MM-DD' (no LPAD!)
    def _ph_date_repl(m):
        day = int(m.group(1))
        # first day of current month + (day-1)
        if day <= 1:
            return f"DATE_TRUNC(CURRENT_DATE('{LOCAL_TZ}'), MONTH)"
        return f"DATE_ADD(DATE_TRUNC(CURRENT_DATE('{LOCAL_TZ}'), MONTH), INTERVAL {day-1} DAY)"

    # catches 'YYYY-MM-01', 'YYYY-MM-15', etc
    sql_query = re.sub(r"(?i)\b'YYYY-MM-(\d{2})'\b", _ph_date_repl, sql_query)

    # Wrap real '2024-03-01' → DATE('2024-03-01')
    sql_query = re.sub(
        r"(?<!DATE\()\b'(\d{4}-\d{2}-\d{2})'\b",
        r"DATE('\1')",
        sql_query,
    )

    return sql_query


# ──────────────────────────────────────────────────────────────────────────────
# FIX WINDOW ORDER BY ERRORS
# ──────────────────────────────────────────────────────────────────────────────
def fix_window_order_by(sql: str) -> str:
    """
    Only fix LAG/LEAD: ensure OVER(...) contains ORDER BY.
    Do NOT remove ORDER BY from other window functions (BigQuery allows it).
    """
    s = sql
    pattern = re.compile(r"\b(LAG|LEAD)\s*\(", re.IGNORECASE)

    i = 0
    while True:
        m = pattern.search(s, i)
        if not m:
            break

        # find "OVER" after this LAG/LEAD call
        over_m = re.search(r"\bOVER\s*\(", s[m.end():], re.IGNORECASE)
        if not over_m:
            i = m.end()
            continue

        over_pos = m.end() + over_m.start()  # points to 'OVER'
        paren_pos = s.find("(", over_pos)
        if paren_pos == -1:
            i = m.end()
            continue

        # extract balanced (...) after OVER(
        depth = 0
        j = paren_pos
        while j < len(s):
            if s[j] == "(":
                depth += 1
            elif s[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1

        if depth != 0:
            i = m.end()
            continue

        over_inner = s[paren_pos + 1 : j]
        if not re.search(r"\bORDER\s+BY\b", over_inner, re.IGNORECASE):
            # add ORDER BY 1 at the end of OVER(...)
            new_inner = over_inner.rstrip() + " ORDER BY 1"
            s = s[:paren_pos + 1] + new_inner + s[j:]
            i = paren_pos + 1 + len(new_inner) + 1
        else:
            i = j + 1

    return s


# ──────────────────────────────────────────────────────────────────────────────
# EXECUTOR
# ──────────────────────────────────────────────────────────────────────────────
def execute_cached_query(sql_query: str):
    cache_key = get_cache_key(sql_query)
    now = time.time()

    if cache_key in query_cache:
        df, ts = query_cache[cache_key]
        if now - ts < cache_ttl:
            return df

    job = bq_client.query(sql_query)
    df = job.result().to_dataframe()

    query_cache[cache_key] = (df.copy(), now)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# AI FIELD MATCHING
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=100)
def find_matches_with_ai_cached(instruction: str, smap_json: str):
    smap = json.loads(smap_json)

    prompt = f"""
Знайди всі поля, які згадує користувач:

{json.dumps(smap, indent=2)}

Текст:
"{instruction}"

Поверни список "field:value", через кому.
"""

    try:
        resp = model.generate_content(prompt, generation_config={"temperature": 0})
        txt = resp.text.strip()
        if txt == "NONE":
            return []
        out = []
        for part in txt.split(","):
            if ":" in part:
                f, v = part.strip().split(":", 1)
                out.append((f, v))
        return out
    except Exception:
        return []


def find_matches_with_ai(instruction, smap):
    return find_matches_with_ai_cached(instruction, json.dumps(smap, sort_keys=True))


# ──────────────────────────────────────────────────────────────────────────────
# SPLIT
# ──────────────────────────────────────────────────────────────────────────────
def split_into_separate_queries(message: str) -> list:
    try:
        prompt = f"""
Розбий текст на окремі запити:

"{message}"

Формат:
ЗАПИТ_1: ...
ЗАПИТ_2: ...
"""
        resp = model.generate_content(prompt, generation_config={"temperature": 0})
        lines = resp.text.strip().split("\n")
        out = []
        for ln in lines:
            if ln.startswith("ЗАПИТ_"):
                q = ln.split(":", 1)[1].strip()
                out.append(q)
        return out if out else [message]
    except Exception:
        return [message]


# ──────────────────────────────────────────────────────────────────────────────
# SQL GENERATOR + METRIC PARSER INTEGRATION
# ──────────────────────────────────────────────────────────────────────────────
def generate_sql(instruction_part: str, smap) -> str:
    """
    Тут ми вставляємо metric_parser.detect_metric + metric_loader.get_metrics
    і даємо SQL-генерації підказку з метрикою.
    """

    # 1. Детекція метрики
    metric = detect_metric(instruction_part)
    metrics = get_metrics()

    metric_hint = f"\nВизначена метрика: {metric}\n" if metric else ""

    rev_schema, cost_schema = get_all_schemas()
    date_cols = _collect_date_columns(rev_schema) | _collect_date_columns(cost_schema)

    rev_cols = ", ".join([c["name"] for c in rev_schema]) if rev_schema else "(немає схеми REVENUE)"
    cost_cols = ", ".join([c["name"] for c in cost_schema]) if cost_schema else "(немає схеми COST)"

    sql_prompt = f"""
Згенеруй BigQuery SQL для завдання:

"{instruction_part}"

{metric_hint}

Повні назви таблиць:
REVENUE_TABLE = `{REVENUE_TABLE_REF}`
COST_TABLE    = `{COST_TABLE_REF}`

Доступні поля (метрики):
{metrics}

Стовпці таблиці REVENUE (реальні назви колонок):
{rev_cols}

Стовпці таблиці COST (реальні назви колонок):
{cost_cols}

Схема REVENUE:
{json.dumps(rev_schema, indent=2)}

Схема COST:
{json.dumps(cost_schema, indent=2)}

Правила:
- Використовуй ТІЛЬКИ ті поля, які є в списках колонок вище. Не вигадуй нових полів (наприклад, event_type), якщо їх немає в схемі.
- Якщо запит про "opex", "cost", "витрати", "спенд" — використовуй таблицю `{COST_TABLE_REF}`.
- Якщо запит про revenue, дохід, GMV — використовуй таблицю `{REVENUE_TABLE_REF}`.
- Для агрегатів (SUM, AVG, COUNT, тощо) завжди став alias, наприклад: SELECT SUM(revenue) AS value.
- Не залишай SELECT SUM(...) без alias, щоб назва колонки не була f0_.
- Використовуй тільки BigQuery SQL.
- Не використовуй STRFTIME.
- Використовуй CURRENT_DATE('{LOCAL_TZ}').
- Не пиши ORDER BY у window функціях, крім випадків, коли це LAG/LEAD (BigQuery вимагає ORDER BY для цих функцій).
- Поверни лише SQL без пояснень і без Markdown.
"""

    resp = model.generate_content(sql_prompt, generation_config={"temperature": 0})

    # ✨ Sanitize raw LLM output first
    sql = sanitize_llm_sql(resp.text)

    # ✨ Fix window functions (LAG/LEAD order issues)
    sql = fix_window_order_by(sql)

    # ✨ Fix dates and casts
    sql = _sanitize_sql_dates(sql, date_cols)

    return sql


# ──────────────────────────────────────────────────────────────────────────────
# EXECUTE SINGLE QUERY
# ──────────────────────────────────────────────────────────────────────────────
def execute_single_query(instruction: str, smap: dict, user_id: str = "unknown") -> str:
    instruction_part = instruction.strip()
    if not instruction_part:
        return "Повідомлення порожнє."

    matched = find_matches_with_ai(instruction_part, smap)
    for field, value in matched:
        instruction_part += f" ({field}='{value}')"

    sql_query = generate_sql(instruction_part, smap)

    try:
        df = execute_cached_query(sql_query)
    except Exception as e:
        msg = str(e)
        if RETURN_SQL_ON_ERROR:
            return f"❌ SQL ERROR:\n```sql\n{sql_query}\n```\n{msg}"
        return f"❌ Помилка при виконанні SQL:\n{msg}"

    if df.empty:
        return "Результат порожній."

    # Якщо BigQuery повернув одну колонку з f0_
    if len(df.columns) == 1 and str(df.columns[0]).startswith("f0_"):
        df = df.rename(columns={df.columns[0]: "value"})

    # ======================================================================
    # TABLE RENDER (MARKDOWN)
    # ======================================================================
    def render_table(df: pd.DataFrame) -> str:
        col_widths = {
            col: max(df[col].astype(str).map(len).max(), len(col))
            for col in df.columns
        }

        header = "| " + " | ".join(f"{col:{col_widths[col]}}" for col in df.columns) + " |"
        separator = "|-" + "-|-".join("-" * col_widths[col] for col in df.columns) + "-|"

        rows = []
        for _, row in df.iterrows():
            rows.append("| " + " | ".join(f"{str(row[col]):{col_widths[col]}}" for col in df.columns) + " |")

        return "\n".join([header, separator] + rows)

    # ======================================================================
    # ASCII CHART
    # ======================================================================
    def render_ascii_chart(df: pd.DataFrame) -> str:
        import numpy as np

        # numeric value column
        num_cols = df.select_dtypes(include=["float", "int"]).columns
        if len(num_cols) == 0:
            return ""

        val_col = num_cols[0]

        # date-like label column
        date_cols = [c for c in df.columns if "date" in c.lower() or "month" in c.lower()]
        if len(date_cols) == 0:
            date_cols = [df.columns[0]]

        x_col = date_cols[0]

        values = df[val_col].fillna(0).tolist()
        labels = df[x_col].astype(str).tolist()

        max_len = 40
        max_val = max(values) if max(values) > 0 else 1

        lines = ["📈 *ASCII графік*"]
        for label, val in zip(labels, values):
            bar_len = int((val / max_val) * max_len)
            bar = "█" * bar_len
            lines.append(f"{label:10} | {bar} {val}")

        return "\n".join(lines)

    # ======================================================================
    # Compose final Slack message
    # ======================================================================
    table_md = render_table(df)
    ascii_md = render_ascii_chart(df)

    final_display = f"```\n{table_md}\n```\n{ascii_md}"

    # ======================================================================
    # Vertex analysis — unchanged
    # ======================================================================
    analysis_prompt = f"""
Проаналізуй результат CSV нижче:

{df.to_csv(index=False)}

Інструкція користувача:
"{instruction_part}"

Зроби короткий висновок (3–4 речення).
"""
    resp = model.generate_content(analysis_prompt, generation_config={"temperature": 0})

    return final_display + "\n\n" + resp.text.strip()
# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY
# ──────────────────────────────────────────────────────────────────────────────
def process_slack_message(message: str, smap: dict, user_id: str = "unknown") -> str:
    queries = split_into_separate_queries(message)

    if len(queries) == 1:
        return execute_single_query(queries[0], smap, user_id)

    out = f"📝 Знайдено {len(queries)} запитів:\n\n"
    for i, q in enumerate(queries, 1):
        ans = execute_single_query(q, smap, user_id)
        out += f"**Запит {i}:** {q}\n{ans}\n\n"
    return out


def run_analysis(message: str, semantic_map_override=None, user_id="unknown"):
    smap = semantic_map_override or semantic_map
    return process_slack_message(message, smap, user_id)
