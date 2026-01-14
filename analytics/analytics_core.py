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
from datetime import datetime

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

REVENUE_METRICS = {
    "revenue", "gmv", "gross_revenue",
    "gross_usd", "total_revenue"
}

def extract_account_no(text: str) -> int | None:
    m = re.search(
        r"(рахунк(у|ом)?|account|acct)\s*(№|number)?\s*(\d{4,10})",
        text.lower()
    )
    if m:
        return int(m.group(4))
    return None

EVENT_TYPE_BY_INTENT = {
    "trial": "trial",
    "тріал": "trial",
    "subscription": "sale",
    "subscriptions": "sale",
    "підписка": "sale",
    "підписки": "sale",
    "purchase": "sale",
    "покупка": "sale",
    "vat": "vat",
    "tax": "vat",
    "refund": "refund",
    "refunds": "refund",
    "повернення": "refund",
    "chargeback": "chargeback",
    "чарджбек": "chargeback",
    "commission": "commission",
    "комісія": "commission",
}

def extract_year(text: str) -> int | None:
    m = re.search(r"\b(20\d{2})\b", text)
    if m:
        y = int(m.group(1))
        if 2000 <= y <= 2100:
            return y
    return None

def detect_event_type(text: str) -> str | None:
    text = text.lower()
    for keyword, event_type in EVENT_TYPE_BY_INTENT.items():
        if keyword in text:
            return event_type
    return None

def _needs_breakdown(text: str) -> bool:
    keywords = [
        "розбив", "breakdown",
        "по центрах", "по категоріях",
        "by center", "by category"
    ]
    t = text.lower()
    return any(k in t for k in keywords)

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

def _schema_has_column(schema_list, col_name: str) -> bool:
    col_name = col_name.lower()
    return any((c.get("name") or "").lower() == col_name for c in (schema_list or []))

def _ensure_where_filter(sql: str, condition_sql: str) -> str:
    sql_lower = sql.lower()
    if condition_sql.lower() in sql_lower:
        return sql
    if " where " in f" {sql_lower} ":
        return re.sub(
            r"\bwhere\b",
            f"WHERE {condition_sql} AND",
            sql,
            flags=re.IGNORECASE,
            count=1,
        )
    return re.sub(
        r"(\bfrom\b\s+`?[\w\-\.:]+`?)",
        r"\1 WHERE " + condition_sql,
        sql,
        flags=re.IGNORECASE,
        count=1,
    )

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
    sql_query = re.sub(
        r"CURRENT_DATE\s*\(\s*([A-Za-z]+\/[A-Za-z_]+)\s*\)",
        r"CURRENT_DATE('\1')",
        sql_query,
        flags=re.IGNORECASE,
    )
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
    for col in date_columns:
        pattern = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"
        def _unwrap(m):
            inner = m.group(1)
            clean = inner.strip("`")
            if clean == col or clean.endswith(f".{col}"):
                return inner
            return m.group(0)
        sql_query = re.sub(pattern, _unwrap, sql_query, flags=re.IGNORECASE)

    sql_query = re.sub(
        r"'YYYY-MM-DD'",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )
    sql_query = re.sub(
        r"'YYYY-MM-01'",
        f"DATE_TRUNC(CURRENT_DATE('{LOCAL_TZ}'), MONTH)",
        sql_query,
        flags=re.IGNORECASE,
    )
    sql_query = re.sub(
        r"'YYYY-MM-31'",
        f"LAST_DAY(CURRENT_DATE('{LOCAL_TZ}'))",
        sql_query,
        flags=re.IGNORECASE,
    )
    return sql_query

def _sanitize_division_by_zero(sql: str) -> str:
    strings = {}
    def protect(m):
        k = f"/*__STR_{len(strings)}__*/"
        strings[k] = m.group(0)
        return k
    sql = re.sub(r"'[^']*'", protect, sql)
    sql = re.sub(
        r"\b(CURRENT_DATE|DATE|DATETIME|TIMESTAMP)\s*\([^)]*\)",
        protect,
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(r"\b[A-Za-z_]+/[A-Za-z_]+\b", protect, sql)
    sql = re.sub(
        r"""(?<!SAFE_DIVIDE\()(?<!SUM\()(?<!AVG\()(?<!COUNT\()(?P<a>\b[\w\.]+\b)\s*/\s*(?P<b>\b[\w\.]+\b)""",
        r"SAFE_DIVIDE(\g<a>, \g<b>)",
        sql,
        flags=re.VERBOSE | re.IGNORECASE,
    )
    for k, v in strings.items():
        sql = sql.replace(k, v)
    return sql

def fix_window_order_by(sql: str) -> str:
    pattern = re.compile(
        r"""(?P<fn>\b(?:LAG|LEAD)\s*\(.*?\))\s*OVER\s*\((?P<inside>[^)]*)\)""",
        re.IGNORECASE | re.DOTALL | re.VERBOSE,
    )
    def _add_order_by(m: re.Match) -> str:
        fn = m.group("fn")
        inside = m.group("inside")
        if re.search(r"\bORDER\s+BY\b", inside, re.IGNORECASE):
            return m.group(0)
        inside_fixed = (inside.strip() + " ORDER BY 1").strip()
        return f"{fn} OVER ({inside_fixed})"
    return pattern.sub(_add_order_by, sql)

def requires_date_range(text: str) -> bool:
    keywords = [
        "збільш", "зменш", "вирос", "впав",
        "increase", "decrease", "grow", "drop",
        "динамік", "тренд", "trend",
        "порівня", "compare",
        "чи більше", "чи менше",
        "has increased", "has decreased"
    ]
    t = text.lower()
    return any(k in t for k in keywords)

def has_explicit_date(text: str) -> bool:
    return bool(re.search(
        r"\b(20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|"
        r"січ|лют|бер|кві|тра|чер|лип|сер|вер|жов|лис|гру|"
        r"місяц|квартал|рік|"
        r"from|to|between|до|від)\b",
        text.lower()
    ))
    
def is_trend_question(text: str) -> bool:
    return bool(re.search(
        r"(рост|пад|зрост|зменш|динамік|trend|increase|decrease).*(чи|\?|vs|порівня)",
        text.lower()
    ))

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

@lru_cache(maxsize=100)
def find_matches_with_ai_cached(instruction: str, smap_json: str):
    smap = json.loads(smap_json)
    prompt = f"""
Знайди всі поля, які згадує користувач:
{json.dumps(smap, indent=2)}
Текст: "{instruction}"
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
# SPLIT (UPDATED)
# ──────────────────────────────────────────────────────────────────────────────
def _has_filter_only_tail(text: str) -> bool:
    t = text.lower()
    filter_patterns = [
        r"за\s+контрагентом\s+\w+",
        r"по\s+контрагенту\s+\w+",
        r"by\s+vendor\s+\w+",
        r"for\s+vendor\s+\w+",
    ]
    has_filter = any(re.search(p, t) for p in filter_patterns)
    has_split_words = re.search(r"\b(і|та|also|and)\b", t)
    return has_filter and not has_split_words
    
def split_into_separate_queries(message: str) -> list:
    if extract_account_no(message) is not None and not is_trend_question(message):
        return [message]

    if _has_filter_only_tail(message):
        return [message]

    try:
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        prompt = f"""
Ти — експертний аналітик. Твоє завдання — визначити, чи містить повідомлення користувача ДЕКІЛЬКА РІЗНИХ питань, чи це ОДНЕ складне питання.
Сьогоднішня дата: {current_date_str}

ПРАВИЛА (CRITICAL):
1. НЕ РОЗБИВАЙ запит, якщо частини є уточненнями (фільтри часу, групування, умови).
   - "Покажи дохід за останні 3 місяці потижнево" -> ЦЕ ОДИН ЗАПИТ. (Тут є метрика + час + групування).
   - "Який дохід у травні та який у червні" -> ЦЕ ДВА ЗАПИТИ.
   - "Дохід по країнах за 2024 рік" -> ЦЕ ОДИН ЗАПИТ.
2. Фільтри часу ("останні 3 місяці", "вчора", "минулого тижня") ЗАВЖДИ повинні залишатися разом із метрикою, до якої вони відносяться.
3. Інструкції з групування ("потижнево", "по центрах", "weekly") ЗАВЖДИ залишаються в основному запиті.

Повідомлення: "{message}"

Якщо це один запит, поверни його ж.
Якщо декілька, поверни у форматі:
ЗАПИТ_1: ...
ЗАПИТ_2: ...
"""
        resp = model.generate_content(prompt, generation_config={"temperature": 0})
        text_resp = resp.text.strip()
        
        if "ЗАПИТ_" not in text_resp:
             return [message]

        lines = text_resp.split("\n")
        out = []
        for ln in lines:
            if ln.startswith("ЗАПИТ_"):
                q = ln.split(":", 1)[1].strip()
                out.append(q)
        return out if out else [message]
    except Exception:
        return [message]


# ──────────────────────────────────────────────────────────────────────────────
# SQL GENERATOR + METRIC PARSER INTEGRATION (UPDATED)
# ──────────────────────────────────────────────────────────────────────────────
def generate_sql(instruction_part: str, smap) -> str:
    today_str = datetime.now().strftime('%Y-%m-%d')   
    account_no = extract_account_no(instruction_part)
    year = extract_year(instruction_part)
    
    metric = detect_metric(instruction_part)
    metrics = get_metrics()
    metric_hint = f"\nВизначена метрика: {metric}\n" if metric else ""

    rev_schema, cost_schema = get_all_schemas()
    date_cols = _collect_date_columns(rev_schema) | _collect_date_columns(cost_schema)

    rev_cols = ", ".join([c["name"] for c in rev_schema]) if rev_schema else "(немає схеми REVENUE)"
    cost_cols = ", ".join([c["name"] for c in cost_schema]) if cost_schema else "(немає схеми COST)"

    sql_prompt = f"""
Згенеруй BigQuery SQL для завдання.
Поточна дата: {today_str}

Завдання: "{instruction_part}"

{metric_hint}

Повні назви таблиць:
REVENUE_TABLE = `{REVENUE_TABLE_REF}`
COST_TABLE    = `{COST_TABLE_REF}`

Доступні поля (метрики):
{metrics}

Стовпці REVENUE: {rev_cols}
Стовпці COST: {cost_cols}

Схеми таблиць (JSON):
REVENUE: {json.dumps(rev_schema, indent=2)}
COST: {json.dumps(cost_schema, indent=2)}

Правила SQL:
1. ЧАСОВІ ФІЛЬТРИ ("останні 3 місяці", "минулий рік" тощо):
   - Використовуй поле дати (наприклад `order_date`, `date`, `created_at` — яке є в схемі).
   - Для "останні X місяців" використовуй: `WHERE date_column >= DATE_SUB(CURRENT_DATE('{LOCAL_TZ}'), INTERVAL X MONTH)`.
   - Не використовуй `BETWEEN` зі статичними датами, якщо просять відносний період ("останні...").

2. ГРУПУВАННЯ ЧАСУ ("потижнево", "weekly", "по місяцях"):
   - Для "потижнево": `GROUP BY DATE_TRUNC(date_column, WEEK)`, у SELECT додай `DATE_TRUNC(date_column, WEEK) AS week_start`.
   - Для "по місяцях": `GROUP BY DATE_TRUNC(date_column, MONTH)`.
   - Обов'язково додай `ORDER BY week_start ASC` (або month_start) для графіків.

3. ЗАГАЛЬНІ:
   - Використовуй ТІЛЬКИ поля зі схеми вище. Не вигадуй нових полів.
   - Якщо запит про "revenue/дохід" — таблиця `{REVENUE_TABLE_REF}`. Якщо "cost/витрати" — `{COST_TABLE_REF}`.
   - Для агрегатів завжди давай alias (наприклад `total_revenue`).
   - Якщо питають "скільки" або "sum" БЕЗ уточнення "по днях/тижнях/категоріях" — НЕ використовуй GROUP BY.
   - Якщо запит про trial / тріали, то використовувати таблицю `{REVENUE_TABLE_REF}` і в ній брати `event_name = "sale"` і `product_id LIKE "%trial%"`.
   - Якщо запит про айді рахунку, або питається про "рахунок", то — використовуй таблицю `{COST_TABLE_REF}` і в ній поле `account_no`.
   - Поверни ТІЛЬКИ SQL код.
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

    if (
        account_no is not None
        and year is not None
        and re.search(r"\b(скільки|sum|total)\b", instruction_part.lower())
        and not _needs_breakdown(instruction_part)
    ):
        preferred = ["posting_date", "date", "dt", "transaction_date"]
        date_col = None
        for c in preferred:
            if _schema_has_column(cost_schema, c):
                date_col = c
                break
    
        if not date_col:
            raise ValueError("No date column found in COST table")
    
        return f"""
        SELECT
            SUM(ABS(amount_lcy)) AS total_expenses
        FROM `{COST_TABLE_REF}`
        WHERE account_no = {account_no}
          AND DATE({date_col}) BETWEEN '{year}-01-01' AND '{year}-12-31'
        """.strip()

    # HARD ENFORCEMENT
    if account_no is not None:
        if REVENUE_TABLE_REF in sql:
            raise ValueError("INVALID SQL: revenue table used for account-based cost query")
    
    if re.search(r"\b(скільки|sum|total)\b", instruction_part.lower()):
        if re.search(r"\bGROUP\s+BY\b", sql, re.IGNORECASE):
            sql = re.sub(r"\bGROUP\s+BY\b.+?$", "", sql, flags=re.IGNORECASE | re.DOTALL)
    
    if metric in {"cost", "opex", "expense", "expenses"}:
        if REVENUE_TABLE_REF in sql:
            raise ValueError("INVALID SQL: revenue table used for cost metric")

    event_type = detect_event_type(instruction_part)
    if _schema_has_column(rev_schema, "event_type"):
        if event_type:
            if f"event_type = '{event_type}'" not in sql.lower():
                sql = _ensure_where_filter(sql, f"event_type = '{event_type}'")
        elif metric in {"subscriptions", "subscription", "count_subscriptions"}:
            sql = _ensure_where_filter(sql, "event_type = 'sale'")

    if account_no is not None:
        sql = _ensure_where_filter(sql, f"account_no = {account_no}")

    if account_no is not None and not _needs_breakdown(instruction_part):
        sql = re.sub(r"\bGROUP\s+BY\b.+?$", "", sql, flags=re.IGNORECASE | re.DOTALL)

    return sql

# ──────────────────────────────────────────────────────────────────────────────
# EXECUTE SINGLE QUERY
# ──────────────────────────────────────────────────────────────────────────────
def execute_single_query(instruction: str, smap: dict, user_id: str = "unknown") -> str:
    instruction_part = instruction.strip()
    if not instruction_part:
        return "Повідомлення порожнє."
        
    if (
        is_trend_question(instruction_part)
        and not has_explicit_date(instruction_part)
       ):
        return (
            "❗ Для аналізу динаміки потрібен часовий період.\n\n"
            "Будь ласка, уточніть, наприклад:\n"
            "• за який місяць?\n"
            "• порівняння яких періодів?\n"
            "• конкретний діапазон дат (від–до)"
        )
    
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

    if len(df.columns) == 1 and str(df.columns[0]).startswith("f0_"):
        df = df.rename(columns={df.columns[0]: "value"})

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
        col_widths = {col: max(df[col].map(len).max(), len(col)) for col in df.columns}
        header = "| " + " | ".join(f"{col:{col_widths[col]}}" for col in df.columns) + " |"
        separator = "|-" + "-|-".join("-" * col_widths[col] for col in df.columns) + "-|"
        rows = []
        for _, row in df.iterrows():
            rows.append("| " + " | ".join(f"{row[col]:{col_widths[col]}}" for col in df.columns) + " |")
        return "\n".join([header, separator] + rows)

    def render_ascii_chart(df: pd.DataFrame, limit: int = 10) -> str:
        df = df.copy()
        num_cols = df.select_dtypes(include=["float", "int"]).columns.tolist()
        if not num_cols:
            return ""
        val_col = num_cols[0]
        label_cols = [c for c in df.columns if c != val_col and df[c].dtype == object]
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

    table_md = render_table(df)
    ascii_md = render_ascii_chart(df)
    final_display = f"```\n{table_md}\n```\n{ascii_md}"

    analysis_prompt = f"""
        Проаналізуй результат аналітичного запиту нижче.
        ЗАБОРОНЕНО:
        - згадувати CSV або файли
        - писати "лише один бізнес у даних"
        - робити припущення про повний датасет
        
        Це агрегований результат виконання SQL-запиту до бази даних.
        Результат може містити один або кілька рядків залежно від умов фільтрації та групування.
        
        Дані:
        {df.to_csv(index=False)}
        
        Запит користувача:
        "{instruction_part}"
        
        Зроби короткий висновок (3–4 речення), описуючи ТІЛЬКИ те, що реально показано в результаті.
        Не роби припущень про повноту або неповноту даних.
"""
    resp = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
    return final_display + "\n\n" + resp.text.strip()

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
