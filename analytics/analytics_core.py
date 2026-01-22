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
from google.api_core.exceptions import BadRequest, GoogleAPIError, NotFound

import vertexai
from vertexai.preview.generative_models import GenerativeModel

from semantic_map import semantic_map

# >>>>>>>>>>>> INTEGRATION (NEW)
from analytics.metric_loader import get_metrics
from analytics.metric_parser import detect_metric
from analytics.trend_analysis import run_trend_analysis
# <<<<<<<<<<<< INTEGRATION END


# ──────────────────────────────────────────────────────────────────────────────
# ENV / LOGGING SETUP
# ──────────────────────────────────────────────────────────────────────────────
BQ_PROJECT         = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET         = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE   = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE      = os.getenv("BQ_COST_TABLE", "cost_test_databot")
VERTEX_LOCATION    = os.getenv("VERTEX_LOCATION", "europe-west1")
LOCAL_TZ           = os.getenv("LOCAL_TZ", "Europe/Kyiv")

BQ_LOG_TABLE       = os.getenv("BQ_LOG_TABLE", f"{BQ_PROJECT}.{BQ_DATASET}.bot_logs")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("ai-bot")

RETURN_SQL_ON_ERROR = os.getenv("RETURN_SQL_ON_ERROR", "false").lower() == "true"

REVENUE_METRICS = {
    "revenue", "gmv", "gross_revenue",
    "gross_usd", "total_revenue"
}

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

model = GenerativeModel("gemini-2.0-flash") 

query_cache = {}
cache_ttl = 300

_schema_cache = {}
_schema_time  = {}
_log_table_checked = False 

# ──────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

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
        "by center", "by category",
        "кожн", "each", "per ",      
        "по ", "by ",                
        "структур", "structure",     
        "розподіл", "distribution",  
        "динамік", "trend",          
        "legal_entity", "юрсоб",
        "retained", "new", "тип", "type" 
    ]
    t = text.lower()
    return any(k in t for k in keywords)

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
    """
    Додає умову в SQL, перевіряючи чи вже існує блок WHERE.
    """
    sql_lower = sql.lower()
    if condition_sql.lower() in sql_lower:
        return sql

    where_match = re.search(r"\bwhere\b", sql_lower)
    
    if where_match:
        idx = where_match.start()
        return sql[:idx+5] + f" {condition_sql} AND " + sql[idx+5:]
    else:
        from_pattern = r"(\bfrom\b\s+`?[\w\-\.:]+`?)"
        match = re.search(from_pattern, sql, flags=re.IGNORECASE)
        if match:
            return re.sub(from_pattern, r"\1 WHERE " + condition_sql, sql, flags=re.IGNORECASE, count=1)
        return sql

# >>> preload schemas
_ = get_all_schemas()


# ──────────────────────────────────────────────────────────────────────────────
# BIGQUERY LOGGING LOGIC
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_log_table_exists():
    global _log_table_checked
    if _log_table_checked:
        return

    try:
        bq_client.get_table(BQ_LOG_TABLE)
        _log_table_checked = True
    except NotFound:
        logger.info(f"Table {BQ_LOG_TABLE} not found. Creating...")
        schema = [
            bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("user_id", "STRING"),
            bigquery.SchemaField("prompt", "STRING"),
            bigquery.SchemaField("sql_query", "STRING"),
            bigquery.SchemaField("response_text", "STRING"),
            bigquery.SchemaField("duration_sec", "FLOAT64"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("error_message", "STRING"),
        ]
        table = bigquery.Table(BQ_LOG_TABLE, schema=schema)
        try:
            bq_client.create_table(table)
            logger.info(f"Table {BQ_LOG_TABLE} created successfully.")
            _log_table_checked = True
        except Exception as e:
            logger.error(f"Failed to create log table: {e}")

def log_interaction(user_id, prompt, sql, response, duration, status, error_msg=None):
    _ensure_log_table_exists()
    try:
        rows = [{
            "event_timestamp": datetime.now().isoformat(),
            "user_id": str(user_id),
            "prompt": str(prompt),
            "sql_query": str(sql) if sql else None,
            "response_text": str(response)[:10000] if response else None,
            "duration_sec": float(duration),
            "status": status,
            "error_message": str(error_msg) if error_msg else None
        }]
        errors = bq_client.insert_rows_json(BQ_LOG_TABLE, rows)
        if errors:
            logger.error(f"BQ Logging errors: {errors}")
    except Exception as e:
        logger.error(f"Failed to write log to BQ: {e}")


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
        r"""(?<!SAFE_DIVIDE\()(?<!SUM\()(?<!AVG\()(?<!COUNT\()(?P<a>[\w\.\`]+)\s*/\s*(?P<b>[\w\.\`]+)""",
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
        if txt == "NONE" or not txt:
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
        # UPDATED PROMPT: More aggressive rules to prevent splitting logic like "new/not present before"
        prompt = f"""
Ти — експертний аналітик. Твоє завдання — визначити, чи містить повідомлення користувача ДЕКІЛЬКА РІЗНИХ питань, чи це ОДНЕ складне питання.
Сьогоднішня дата: {current_date_str}

ПРАВИЛА (CRITICAL):
1. НЕ РОЗБИВАЙ запит, якщо частини є уточненнями (фільтри часу, групування, умови).
2. Фільтри часу ЗАВЖДИ повинні залишатися разом із метрикою.
3. Якщо друге питання питає про ВІДСОТОК, ЧАСТКУ або СПІВВІДНОШЕННЯ (share, percentage) того, що було в першому — НЕ РОЗБИВАЙ.
4. Фрази типу "і до цього їх не було", "та раніше не купували", "які з'явились" — це ВСЕ ОДИН ЗАПИТ (Acquisition/New). НЕ РОЗБИВАЙ.
5. Сполучники "і", "та", "and" всередині речення часто поєднують умови. Якщо сумніваєшся — НЕ РОЗБИВАЙ.

Повідомлення: "{message}"

Якщо це один запит (або послідовний аналіз), поверни його ж без змін.
Якщо декілька НЕЗАЛЕЖНИХ, поверни у форматі:
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
                if q: 
                    out.append(q)
        return out if out else [message]
    except Exception:
        return [message]


# ──────────────────────────────────────────────────────────────────────────────
# SQL GENERATOR
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
1. ЧАСОВІ ФІЛЬТРИ:
   - Для "останні X місяців": `WHERE date_column >= DATE_SUB(CURRENT_DATE('{LOCAL_TZ}'), INTERVAL X MONTH)`.
   - Якщо користувач НЕ вказав період, НЕ додавай умову `WHERE date ...`.

2. ГРУПУВАННЯ ЧАСУ:
   - Для "потижнево": `GROUP BY DATE_TRUNC(date_column, WEEK)`, SELECT `AS week_start`.
   - Обов'язково додай `ORDER BY` для графіків.
   
3. ФІЛЬТРАЦІЯ ТА ВИКЛЮЧЕННЯ (CRITICAL):
   - Якщо "без", "крім", "exclude" -> `AND column != 'value'`.
   - Мапінг країн: "США/USA" -> 'US', "Україна" -> 'UA'.

4. ФІЛЬТРАЦІЯ ПО ТЕКСТУ (account_name):
   - Для категорій витрат використовуй `WHERE account_name LIKE '%Назва%'` в таблиці COST.

5. ТРЕНДИ ТА CTE:
   - Запит ПОВИНЕН бути завершеним `SELECT * FROM CTE_NAME`.
   
6. НОВІ / ACQUISITION (Якщо питають "які нові контрагенти/vendors з'явились, яких не було"):
   - Використовуй логіку: `SELECT vendor_name FROM ... WHERE date >= ... AND vendor_name NOT IN (SELECT vendor_name FROM ... WHERE date < ...)`
   - "Контрагент" зазвичай = `account_name` (в COST) або `user_id` (в REVENUE).

7. ЗАГАЛЬНІ:
   - Використовуй ТІЛЬКИ поля зі схеми.
   - Revenue -> `{REVENUE_TABLE_REF}`, Cost -> `{COST_TABLE_REF}`.
   - Trial -> `event_name = 'sale'` AND `product_id LIKE '%trial%'`.
   - Поверни ТІЛЬКИ SQL код.
"""

    resp = model.generate_content(sql_prompt, generation_config={"temperature": 0})
    sql = resp.text.strip()
    
    sql = re.sub(r"```sql|```", "", sql).strip()

    match = re.search(r"(SELECT|WITH)\s.*", sql, re.IGNORECASE | re.DOTALL)
    if match:
        sql = match.group(0)
        
    sql = fix_window_order_by(sql)
    sql = _sanitize_sql_dates(sql, date_cols)
    sql = _sanitize_division_by_zero(sql)

    sql = re.sub(r"'\s*[\r\n]+\s*'", " ", sql)
    sql = re.sub(r'"\s*[\r\n]+\s*"', " ", sql)

    if (
        account_no is not None
        and year is not None
        and re.search(r"\b(скільки|sum|total)\b", instruction_part.lower())
        and not _needs_breakdown(instruction_part)
    ):
        preferred = ["posting_date", "date", "dt", "transaction_date"]
        date_col = next((c for c in preferred if _schema_has_column(cost_schema, c)), None)
        if not date_col: raise ValueError("No date column found in COST table")
    
        return f"SELECT SUM(ABS(amount_lcy)) AS total_expenses FROM `{COST_TABLE_REF}` WHERE account_no = {account_no} AND DATE({date_col}) BETWEEN '{year}-01-01' AND '{year}-12-31'"

    if account_no is not None and REVENUE_TABLE_REF in sql:
        sql = sql.replace(f"`{REVENUE_TABLE_REF}`", f"`{COST_TABLE_REF}`")
    
    if metric in {"cost", "opex", "expense", "expenses"} and REVENUE_TABLE_REF in sql:
          sql = sql.replace(f"`{REVENUE_TABLE_REF}`", f"`{COST_TABLE_REF}`")

    event_type = detect_event_type(instruction_part)
    if _schema_has_column(rev_schema, "event_type"):
        if event_type:
            sql = _ensure_where_filter(sql, f"event_type = '{event_type}'")
        elif metric in {"subscriptions", "subscription", "count_subscriptions"}:
            sql = _ensure_where_filter(sql, "event_type = 'sale'")

    if account_no is not None:
        sql = _ensure_where_filter(sql, f"account_no = {account_no}")

    cleaned_start = sql.strip().upper()
    if not (cleaned_start.startswith("SELECT") or cleaned_start.startswith("WITH")):
        raise ValueError(f"🤖 Відповідь AI (не SQL):\n\n{sql}")

    return sql

# ──────────────────────────────────────────────────────────────────────────────
# EXECUTE SINGLE QUERY (WITH INTEGRATED TOKEN LIMIT FIX & LOGGING)
# ──────────────────────────────────────────────────────────────────────────────
def execute_single_query(instruction: str, smap: dict, user_id: str = "unknown") -> str:
    start_time = time.time()
    instruction_part = instruction.strip()
    
    generated_sql = None
    status = "SUCCESS"
    error_details = None
    final_response = ""

    TOKEN_LIMIT_MSG = (
        "⚠️ **Обмеження контексту (Token Limit Exceeded)**\n\n"
        "Ми зіткнулися з обмеженням контекстного вікна. "
        "Спробуйте розбити запит на менші частини або скоротити період."
    )

    try:
        if not instruction_part:
            return "Повідомлення порожнє."
            
        if (is_trend_question(instruction_part) and not has_explicit_date(instruction_part)):
            final_response = (
                "❗ Для аналізу динаміки потрібен часовий період.\n\n"
                "Будь ласка, уточніть період (наприклад, за травень або за останні 3 місяці)."
            )
            return final_response
        
        matched = find_matches_with_ai(instruction_part, smap)
        augmented_instruction = instruction_part
        for field, value in matched:
            augmented_instruction += f" ({field}='{value}')"

        try:
            generated_sql = generate_sql(augmented_instruction, smap)
        except Exception as e:
            err_str = str(e).lower()
            if any(k in err_str for k in ["429", "exhausted", "token", "quota"]):
                status = "TOKEN_LIMIT"
                return TOKEN_LIMIT_MSG
            raise e

        # ВИКОНАННЯ ЗАПИТУ
        df = execute_cached_query(generated_sql)

        if df.empty:
            final_response = "Результат порожній."
        else:
            if len(df.columns) == 1 and str(df.columns[0]).startswith("f0_"):
                df = df.rename(columns={df.columns[0]: "value"})

            # -- DETECTION OF TRANSACTION LIST --
            # If the data looks like a raw transaction list for a specific entity (user/account),
            # we want a detailed textual breakdown instead of a giant ASCII table.
            
            is_detailed_transaction_list = False
            col_names = [c.lower() for c in df.columns]
            has_user_id = any(x in col_names for x in ['user_id', 'email', 'account_no', 'customer_id'])
            has_date = any(x in col_names for x in ['date', 'order_date', 'transaction_date', 'event_date', 'posting_date'])
            # Threshold: less than 30 rows (to avoid hitting token limits with large lists)
            if has_user_id and has_date and len(df) < 30 and len(df) > 0:
                is_detailed_transaction_list = True

            final_display = ""
            analysis_prompt = ""
            data_for_ai = df.head(50).to_csv(index=False)

            if is_detailed_transaction_list:
                # --- MODE 1: DETAILED TEXTUAL BREAKDOWN (No ASCII Table) ---
                # This matches the user request for "Screens 2, 3, 4" style
                analysis_prompt = f"""
Ти — старший фінансовий аналітик Headway. 
Твоє завдання — проаналізувати транзакції конкретного користувача/контрагента і вивести їх у чіткому структурованому форматі.

Дані (CSV): 
{data_for_ai}

Запит користувача: "{instruction_part}"

ФОРМАТ ВІДПОВІДІ (CRITICAL):
Не малюй таблицю. Виведи нумерований список транзакцій з аналізом.

1. **Заголовок**: "Аналіз транзакцій [ID користувача/назва]"
2. **Список транзакцій** (для кожної транзакції окремий пункт):
   1. **[Тип транзакції]** (наприклад: "Початкова підписка", "Продовження", "Повернення").
      * **Дата**: YYYY-MM-DD
      * **Сума**: [Сума] [Валюта]
      * **Продукт**: [Назва продукту]
      * **Період**: [Період, якщо є]
      * **Деталі**: (Платформа, Країна, Статус) - якщо є в даних.
      
3. **Висновки фінансового аналітика** (після списку):
   * **LTV / Загальні витрати**: Порахуй суму всіх успішних транзакцій.
   * **Поведінка**: Опиши життєвий цикл (наприклад: "Користувач зайшов через тріал, потім конвертувався...").
   * **Інше**: Цінова стратегія, географія тощо.

Пиши українською мовою. Використовуй Markdown (bold) для ключів.
"""
            else:
                # --- MODE 2: STANDARD AGGREGATION (Chart + Table + Summary) ---
                def render_table(df: pd.DataFrame, limit: int = 10) -> str:
                    df_copy = df.copy()
                    num_cols = df_copy.select_dtypes(include=["number"]).columns.tolist()
                    if num_cols: df_copy = df_copy.sort_values(by=num_cols[0], ascending=False)
                    df_copy = df_copy.head(limit)
                    for col in num_cols:
                        df_copy[col] = df_copy[col].round(2).map(lambda x: f"{x:,.2f}".replace(",", " ") if pd.notnull(x) else "")
                    df_copy = df_copy.astype(str)
                    col_widths = {col: max(df_copy[col].map(len).max(), len(col)) for col in df_copy.columns}
                    header = "| " + " | ".join(f"{col:{col_widths[col]}}" for col in df_copy.columns) + " |"
                    separator = "|-" + "-|-".join("-" * col_widths[col] for col in df_copy.columns) + "-|"
                    rows = ["| " + " | ".join(f"{row[col]:{col_widths[col]}}" for col in df_copy.columns) + " |" for _, row in df_copy.iterrows()]
                    return "\n".join([header, separator] + rows)

                def render_ascii_chart(df: pd.DataFrame, limit: int = 10) -> str:
                    df_copy = df.copy()
                    num_cols = df_copy.select_dtypes(include=["number"]).columns.tolist()
                    if not num_cols: return ""
                    val_col = num_cols[0]
                    label_cols = [c for c in df_copy.columns if c != val_col and df_copy[c].dtype == object]
                    label_col = label_cols[0] if label_cols else df_copy.columns[0]
                    df_copy = df_copy.sort_values(by=val_col, ascending=False).head(limit)
                    values, labels = df_copy[val_col].fillna(0).tolist(), df_copy[label_col].astype(str).tolist()
                    max_val = max(values) if values and max(values) > 0 else 1
                    lines = ["📊 *TOP-10 графік*"]
                    for label, val in zip(labels, values):
                        bar = "█" * int((val / max_val) * 30)
                        lines.append(f"{label[:12]:12} | {bar:<30} {val:,.2f}".replace(",", " "))
                    return "\n".join(lines)

                table_md = render_table(df)
                ascii_md = render_ascii_chart(df)
                final_display = f"```\n{table_md}\n```\n{ascii_md}\n\n"

                analysis_prompt = f"""
Ти — старший фінансовий аналітик Headway. Твоє завдання — проаналізувати отримані дані та дати чітку, професійну відповідь.

Дані (CSV, перші 50 рядків): 
{data_for_ai}

Запит користувача: "{instruction_part}"

ІНСТРУКЦІЇ (CRITICAL):
1. Обов'язково розрахуй частки (відсотки) та пропорції, якщо це доречно.
2. Якщо у даних є чітке домінування, обов'язково акцентуй на цьому.
3. Пиши короткими тезами (буллітами).
4. Якщо бачиш аномалії або важливі тренди — виділи їх окремо.
5. Дай інсайт, а не просто переказуй цифри.
"""
            
            try:
                resp = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
                final_response = final_display + resp.text.strip()
            except Exception as e:
                if any(k in str(e).lower() for k in ["429", "exhausted", "token"]):
                    final_response = final_display + TOKEN_LIMIT_MSG
                else:
                    final_response = final_display + "\n(Висновок не згенеровано через помилку)"

    except Exception as e:
        status = "ERROR"
        error_details = str(e)
        if "🤖 Відповідь AI" in error_details:
            final_response = error_details.replace("ValueError: ", "")
            status = "SUCCESS" 
        else:
            if RETURN_SQL_ON_ERROR and generated_sql:
                final_response = f"❌ SQL ERROR:\n```sql\n{generated_sql}\n```\n{error_details}"
            else:
                final_response = f"❌ Помилка при виконанні SQL або аналізу."

    finally:
        log_interaction(
            user_id=user_id, prompt=instruction_part, sql=generated_sql,
            response=final_response, duration=time.time() - start_time,
            status=status, error_msg=error_details
        )

    return final_response

# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINTS
# ──────────────────────────────────────────────────────────────────────────────
def process_slack_message(message: str, smap: dict, user_id: str = "unknown") -> str:
    queries = split_into_separate_queries(message)
    # FIX: Більш жорстка фільтрація пустих запитів (довжина > 2)
    queries = [q for q in queries if q.strip() and len(q.strip()) > 2]

    if not queries:
        return "Не вдалося розпізнати запит."

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
