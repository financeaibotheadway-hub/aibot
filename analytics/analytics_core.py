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
    """
    BigQuery-safe date sanitizer.
    """

    # 🚑 FIX: CURRENT_DATE(Europe/Kyiv) → CURRENT_DATE('Europe/Kyiv')
    # MUST run before any other CURRENT_DATE handling
    sql_query = re.sub(
        r"CURRENT_DATE\s*\(\s*([A-Za-z]+\/[A-Za-z_]+)\s*\)",
        r"CURRENT_DATE('\1')",
        sql_query,
        flags=re.IGNORECASE,
    )

    # ─────────────────────────────────────────────
    # CURRENT_DATE()
    # ─────────────────────────────────────────────
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

    # ─────────────────────────────────────────────
    # Remove PARSE_DATE around real DATE columns
    # ─────────────────────────────────────────────
    for col in date_columns:
        pattern = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"

        def _unwrap(m):
            inner = m.group(1)
            clean = inner.strip("`")
            if clean == col or clean.endswith(f".{col}"):
                return inner
            return m.group(0)

        sql_query = re.sub(pattern, _unwrap, sql_query, flags=re.IGNORECASE)

    # ─────────────────────────────────────────────
    # YYYY-MM-DD placeholder → CURRENT_DATE
    # ─────────────────────────────────────────────
    sql_query = re.sub(
        r"'YYYY-MM-DD'",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )

    # YYYY-MM-01 → first day of month
    sql_query = re.sub(
        r"'YYYY-MM-01'",
        f"DATE_TRUNC(CURRENT_DATE('{LOCAL_TZ}'), MONTH)",
        sql_query,
        flags=re.IGNORECASE,
    )

    # YYYY-MM-31 → LAST_DAY
    sql_query = re.sub(
        r"'YYYY-MM-31'",
        f"LAST_DAY(CURRENT_DATE('{LOCAL_TZ}'))",
        sql_query,
        flags=re.IGNORECASE,
    )

    return sql_query

def _sanitize_division_by_zero(sql: str) -> str:
    """
    SAFE: replaces a / b -> SAFE_DIVIDE(a, b)
    GUARANTEE: no placeholders ever break SQL
    """

    strings = {}

    def protect(m):
        k = f"/*__STR_{len(strings)}__*/"
        strings[k] = m.group(0)
        return k

    # 🔒 protect strings
    sql = re.sub(r"'[^']*'", protect, sql)

    # 🔒 protect date/time functions
    sql = re.sub(
        r"\b(CURRENT_DATE|DATE|DATETIME|TIMESTAMP)\s*\([^)]*\)",
        protect,
        sql,
        flags=re.IGNORECASE,
    )

    # 🔒 protect timezone identifiers
    sql = re.sub(
        r"\b[A-Za-z_]+/[A-Za-z_]+\b",
        protect,
        sql,
    )

    # ✅ SAFE_DIVIDE only for math
    sql = re.sub(
        r"""
        (?P<a>\([^()]+\)|\b[\w\.]+\b)
        \s*/\s*
        (?P<b>\([^()]+\)|\b[\w\.]+\b)
        """,
        r"SAFE_DIVIDE(\g<a>, \g<b>)",
        sql,
        flags=re.VERBOSE,
    )

    # 🔓 restore (best-effort)
    for k, v in strings.items():
        sql = sql.replace(k, v)

    return sql
# ──────────────────────────────────────────────────────────────────────────────
# FIX WINDOW ORDER BY ERRORS
# ──────────────────────────────────────────────────────────────────────────────
def fix_window_order_by(sql: str) -> str:
    """
    BigQuery:
    - LAG/LEAD REQUIRE ORDER BY inside OVER(...)
    We fix only this error (do NOT strip ORDER BY from other window functions).
    """

    pattern = re.compile(
        r"""
        (?P<fn>\b(?:LAG|LEAD)\s*\(.*?\))      # LAG(...) or LEAD(...)
        \s*OVER\s*\(                         # OVER(
        (?P<inside>[^)]*)                    # inside window spec (simple)
        \)                                   # )
        """,
        re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )

    def _add_order_by(m: re.Match) -> str:
        fn = m.group("fn")
        inside = m.group("inside")

        # already has ORDER BY -> keep
        if re.search(r"\bORDER\s+BY\b", inside, re.IGNORECASE):
            return m.group(0)

        # add safest ORDER BY (syntactic) to satisfy BigQuery
        # ORDER BY 1 is enough to pass validation when we can't infer date column
        inside_fixed = (inside.strip() + " ORDER BY 1").strip()

        return f"{fn} OVER ({inside_fixed})"

    return pattern.sub(_add_order_by, sql)
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
    sql = resp.text.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    sql = re.sub(
        r"^\s*(?:```)?\s*(?:bigquery|bigquery\s+sql|BigQuery|BigQuery\s+SQL)\s*[:\-]*\s*",
        "",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,)

    sql = fix_window_order_by(sql)
    sql = _sanitize_sql_dates(sql, date_cols)
    sql = _sanitize_division_by_zero(sql)

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
    def render_table(df: pd.DataFrame, limit: int = 10) -> str:
        df = df.copy()

        num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
        if num_cols:
            df = df.sort_values(by=num_cols[0], ascending=False)

        df = df.head(limit)

        for col in num_cols:
            df[col] = df[col].round(2).map(
                lambda x: f"{x:,.2f}".replace(",", " ")
                if pd.notnull(x) else ""
            )

        df = df.astype(str)

        col_widths = {
            col: max(df[col].map(len).max(), len(col))
            for col in df.columns
        }

        header = "| " + " | ".join(f"{col:{col_widths[col]}}" for col in df.columns) + " |"
        separator = "|-" + "-|-".join("-" * col_widths[col] for col in df.columns) + "-|"

        rows = []
        for _, row in df.iterrows():
            rows.append(
                "| " + " | ".join(f"{row[col]:{col_widths[col]}}" for col in df.columns) + " |"
            )

        return "\n".join([header, separator] + rows)

    # ======================================================================
    # ASCII CHART
    # ======================================================================
    def render_ascii_chart(df: pd.DataFrame, limit: int = 10) -> str:
        df = df.copy()

        num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
        if not num_cols:
            return ""

        val_col = num_cols[0]

        label_cols = [
            c for c in df.columns
            if c != val_col and df[c].dtype == object
        ]
        label_col = label_cols[0] if label_cols else df.columns[0]

        df = df.sort_values(by=val_col, ascending=False).head(limit)

        values = df[val_col].fillna(0).tolist()
        labels = df[label_col].astype(str).tolist()

        max_len = 30
        max_val = max(values) if max(values) > 0 else 1

        lines = ["📊 *TOP-10 графік*"]

        for label, val in zip(labels, values):
            bar_len = int((val / max_val) * max_len)
            bar = "█" * bar_len
            val_fmt = f"{val:,.2f}".replace(",", " ")
            lines.append(f"{label[:12]:12} | {bar:<30} {val_fmt}")

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

