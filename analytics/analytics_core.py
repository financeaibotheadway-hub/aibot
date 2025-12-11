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
# DATE TOOLS
# ──────────────────────────────────────────────────────────────────────────────
def _collect_date_columns(schema_list):
    return {
        f["name"]
        for f in schema_list
        if f.get("type") in ("DATE", "DATETIME", "TIMESTAMP")
    }


def _sanitize_sql_dates(sql_query: str, date_columns: set) -> str:
    sql_original = sql_query

    sql_query = re.sub(
        r"\bCURRENT_DATE\s*\(\s*\)",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query, flags=re.IGNORECASE)

    sql_query = re.sub(
        r"\bCURRENT_DATE\b(?!\s*\()",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query, flags=re.IGNORECASE)

    # Remove PARSE_DATE around existing DATE fields
    for col in date_columns:
        p1 = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"
        def repl1(m):
            inner = m.group(1)
            clean = inner.strip("`")
            if clean.endswith(f".{col}") or clean == col:
                return inner
            return m.group(0)

        sql_query = re.sub(p1, repl1, sql_query, flags=re.IGNORECASE)

    return sql_query


# ──────────────────────────────────────────────────────────────────────────────
# FIX WINDOW ORDER BY ERRORS
# ──────────────────────────────────────────────────────────────────────────────
def fix_window_order_by(sql: str) -> str:
    """
    BigQuery не дозволяє ORDER BY у window, якщо expression не доступний.
    Ми видаляємо ORDER BY у window-функціях.
    """
    return re.sub(
        r"OVER\s*$begin:math:text$\[\^$end:math:text$]*ORDER\s+BY[^\)]*\)",
        lambda m: re.sub(r"ORDER\s+BY[^\)]*", "", m.group(0)),
        sql,
        flags=re.IGNORECASE
    )


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
    except:
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
                q = ln.split(":")[1].strip()
                out.append(q)
        return out if out else [message]
    except:
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

    sql_prompt = f"""
Згенеруй BigQuery SQL для завдання:

"{instruction_part}"

{metric_hint}

Доступні поля:
{metrics}

Схема REVENUE:
{json.dumps(rev_schema, indent=2)}

Схема COST:
{json.dumps(cost_schema, indent=2)}

Правила:
- Якщо запит про "opex", "cost", "витрати", "спенд" — використовуй таблицю COST.
- Використовуй тільки BigQuery SQL.
- Не використовуй STRFTIME.
- Використовуй CURRENT_DATE('{LOCAL_TZ}').
- Не пиши ORDER BY у window функціях.
- Поверни лише SQL.
"""

    resp = model.generate_content(sql_prompt, generation_config={"temperature": 0})
    sql = resp.text.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()

    sql = fix_window_order_by(sql)
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

    # FINISH → Vertex analysis
    analysis_prompt = f"""
Проаналізуй результат CSV нижче:

{df.to_csv(index=False)}

Інструкція користувача:
"{instruction_part}"

Зроби короткий висновок (3–4 речення).
"""

    resp = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
    return resp.text.strip()


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
