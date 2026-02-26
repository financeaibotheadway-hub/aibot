 
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

# ──────────────────────────────────────────────────────────────────────────────
# SAFE IMPORT BLOCK & INTEGRATION STUBS
# ──────────────────────────────────────────────────────────────────────────────
# Цей блок гарантує, що код не впаде "мовчки", якщо модулі пам'яті відсутні
# або виникає помилка цилічного імпорту.

# 1. Спочатку визначаємо заглушки (Safe defaults)
def _stub_log(q, s, r): return None
def _stub_exact(q): return None
def _stub_similar(q): return ""
def _stub_get_map(): return {}

log_query_to_memory = _stub_log
find_exact_match = _stub_exact
find_similar_matches = _stub_similar
get_semantic_map = _stub_get_map

# 2. Спроба реального імпорту Memory System
try:
    from memory_system import log_query_to_memory, find_exact_match, find_similar_matches
except ImportError:
    # Лише логуємо, але не падаємо
    # logger.warning("⚠️ Memory system not found. Using default stubs.")
    pass

# 3. Спроба реального імпорту Semantic Map
try:
    # Спробуємо імпортувати як функцію
    from semantic_map import get_semantic_map as _real_get_map
    get_semantic_map = _real_get_map
except ImportError:
    try:
        # Fallback: якщо це змінна, обгортаємо її у функцію
        from semantic_map import semantic_map as _map_dict
        def get_semantic_map(): return _map_dict
    except ImportError:
        # logger.warning("⚠️ Semantic map not found. Using empty dict.")
        pass

# >>>>>>>>>>>> INTEGRATION (NEW)
from analytics.metric_loader import get_metrics
from analytics.metric_parser import detect_metric
from analytics.trend_analysis import run_trend_analysis
# <<<<<<<<<<<< INTEGRATION END


# ──────────────────────────────────────────────────────────────────────────────
# ENV / LOGGING SETUP
# ──────────────────────────────────────────────────────────────────────────────
BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")
VERTEX_LOCATION  = os.getenv("VERTEX_LOCATION", "europe-west1")
LOCAL_TZ         = os.getenv("LOCAL_TZ", "Europe/Kyiv")

# Ім'я таблиці для логів
BQ_LOG_TABLE     = os.getenv("BQ_LOG_TABLE", f"{BQ_PROJECT}.{BQ_DATASET}.bot_logs")

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

model = GenerativeModel("gemini-2.5-flash")

query_cache = {}
cache_ttl = 300

_schema_cache = {}
_schema_time  = {}

# Кеш для унікальних значень: (text_hint, json_string, raw_dict)
_db_context_cache = None
_db_context_time = 0
VALUES_CACHE_TTL = 3600  # Оновлювати раз на годину

# Флаг, щоб перевіряти наявність таблиці логів лише 1 раз за запуск
_log_table_checked = False 


# ──────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────────

def extract_account_no(text: str) -> int | None:
    m = re.search(
        r"(?:рахун\w*|account|acct)\s*(?:№|number|#)?\s*(\d{4,10})",
        text.lower()
    )
    if m:
        return int(m.group(1))
    return None

# 🔥 FIX: Розширений список для відмінків (використовуємо корені слів)
EVENT_TYPE_BY_INTENT = {
    "trial": "trial",
    "тріал": "trial",
    "subscription": "sale",
    "subscriptions": "sale",
    "підписк": "sale",  # корінь для підписка, підписки, підписок
    "підпис": "sale",   # ще коротший корінь
    "purchase": "sale",
    "покупк": "sale",   # корінь для покупка, покупки
    "vat": "vat",
    "tax": "vat",
    "refund": "refund",
    "рефанд": "refund",     # Корінь
    "повернен": "refund",   # Корінь
    "chargeback": "chargeback",
    "чарджбек": "chargeback",
    "чарджб": "chargeback", # Корінь (чарджбек, чарджбеків)
    "commission": "commission",
    "комісі": "commission", # Корінь (комісія, комісій, комісію)
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
    # Сортуємо за довжиною ключа (спадання), щоб спочатку шукати довші фрази
    # Це допомагає уникнути помилкових спрацьовувань коротких коренів
    sorted_keywords = sorted(EVENT_TYPE_BY_INTENT.items(), key=lambda x: -len(x[0]))
    
    for keyword, event_type in sorted_keywords:
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
        "legal_entity", "юрсоб"      
    ]
    t = text.lower()
    return any(k in t for k in keywords)

def get_cache_key(query: str) -> str:
    # FIX: Ensure query is a string before encoding
    if not isinstance(query, str):
        query = str(query)
    return hashlib.md5(query.encode("utf-8")).hexdigest()

def get_table_schema(table_ref: str, ttl_sec: int = 3600):
    now = time.time()
    if table_ref not in _schema_cache or now - _schema_time.get(table_ref, 0) > ttl_sec:
        try:
            schema = bq_client.get_table(table_ref).schema
            _schema_cache[table_ref] = [{"name": c.name, "type": c.field_type} for c in schema]
            _schema_time[table_ref] = now
        except Exception:
            return []
    return _schema_cache.get(table_ref, [])

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

# ---------------- NEW FEATURE: FAST CONTEXT SCAN (30 DAYS) ----------------

def _pick_best_date_col(schema_objs, preferred):
    # schema_objs: [{"name":..., "type":...}, ...]
    name_to_type = {f["name"]: f["type"] for f in schema_objs}
    
    for c in preferred:
        if c in name_to_type and name_to_type[c] in ("DATE", "DATETIME", "TIMESTAMP"):
            return c, name_to_type[c]
    # fallback: first date-like
    for f in schema_objs:
        if f["type"] in ("DATE", "DATETIME", "TIMESTAMP"):
            return f["name"], f["type"]
    return None, None

def _date_filter_expr(col_name, col_type):
    # повертає вираз який можна порівнювати з DATE
    if col_type in ("TIMESTAMP", "DATETIME"):
        return f"DATE({col_name})"
    return col_name  # DATE

def get_database_values_context():
    """
    DISABLED FOR PERFORMANCE.
    Вимкнено сканування бази, щоб бот відповідав миттєво.
    """
    # Повертаємо: (текст опису, json рядок, словник значень)
    return "", "{}", {}

# ----------------------------------------------------------------------

def _ensure_where_filter(sql: str, condition_sql: str) -> str:
    """
    Розумне додавання WHERE.
    1. Якщо WHERE вже є -> додаємо AND ...
    2. Якщо WHERE немає -> вставляємо WHERE після FROM, але ПЕРЕД GROUP BY/ORDER BY/LIMIT.
    """
    sql_upper = sql.upper()
    
    # Якщо умова вже є (груба перевірка)
    if condition_sql.upper() in sql_upper:
        return sql

    # Перевіряємо наявність WHERE
    where_match = re.search(r"\bWHERE\b", sql_upper)
    
    if where_match:
        pattern = re.compile(r"\bwhere\b", re.IGNORECASE)
        return pattern.sub(f"WHERE {condition_sql} AND", sql, count=1)
    else:
        keywords_pattern = r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|WINDOW|HAVING|UNION)\b"
        match = re.search(keywords_pattern, sql, re.IGNORECASE)
        
        if match:
            idx = match.start()
            return sql[:idx] + f" WHERE {condition_sql} " + sql[idx:]
        else:
            if "FROM" in sql_upper:
                return sql + f" WHERE {condition_sql}"
            
    return sql

# >>> preload schemas
_ = get_all_schemas()


# ──────────────────────────────────────────────────────────────────────────────
# BIGQUERY LOGGING LOGIC
# ──────────────────────────────────────────────────────────────────────────────
def _ensure_log_table_exists():
    """Створює таблицю логів, якщо її не існує"""
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
    """Записує лог в BigQuery"""
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
    # 1. Handle CURRENT_DATE(WithArg) -> quote it
    sql_query = re.sub(
        r"CURRENT_DATE\s*\(\s*([A-Za-z]+\/[A-Za-z_]+)\s*\)",
        r"CURRENT_DATE('\1')",
        sql_query,
        flags=re.IGNORECASE,
    )
    # 2. Handle empty CURRENT_DATE() -> inject LOCAL_TZ
    sql_query = re.sub(
        r"\bCURRENT_DATE\s*\(\s*\)",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )
    # 3. Handle standalone CURRENT_DATE without parens
    sql_query = re.sub(
        r"\bCURRENT_DATE\b(?!\s*\()",
        f"CURRENT_DATE('{LOCAL_TZ}')",
        sql_query,
        flags=re.IGNORECASE,
    )
    
    # 4. Handle PARSE_DATE unwrapping
    for col in date_columns:
        pattern = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"
        def _unwrap(m):
            inner = m.group(1)
            clean = inner.strip("`")
            if clean == col or clean.endswith(f".{col}"):
                return inner
            return m.group(0)
        sql_query = re.sub(pattern, _unwrap, sql_query, flags=re.IGNORECASE)

    # 5. Placeholders replacement
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
    
    # Protect strings and function calls
    sql = re.sub(r"'[^']*'", protect, sql)
    sql = re.sub(
        r"\b(CURRENT_DATE|DATE|DATETIME|TIMESTAMP)\s*\([^)]*\)",
        protect,
        sql,
        flags=re.IGNORECASE,
    )
    
    # Safer regex
    safe_div_pattern = r"""
        (?P<a>\b[`a-zA-Z0-9_\.]+\b)   # Numerator
        \s*/\s*                       # Division operator
        (?P<b>\b[`a-zA-Z0-9_\.]+\b)   # Denominator
    """
    
    sql = re.sub(
        safe_div_pattern,
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
# SQL GENERATOR
# ──────────────────────────────────────────────────────────────────────────────
def generate_sql(instruction_part: str, smap, semantic_overrides=None) -> str:
    """
    Generates SQL based on instruction.
    PRIORITY OF EVENT_TYPE DETECTION:
    1. Semantic Map Overrides (highest) - if matched by 'find_matches_with_ai'
    2. Rule-based detection (detect_event_type via regex stems)
    3. AI Generation (fallback)
    """
    logger.info(f"Generating SQL for: {instruction_part}")
    
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

    # >>>>> NEW: Get Top Values Context & JSON <<<<<
    # Отримуємо кортеж: (text, json, dict)
    context_data = get_database_values_context()
    
    # Розпаковуємо значення (з обробкою, якщо функція поверне None при помилці)
    if context_data:
        db_context_text, allowed_values_json, _ = context_data
    else:
        db_context_text = "No database values loaded yet."
        allowed_values_json = "{}"

    sql_prompt = f"""
Згенеруй BigQuery SQL для завдання.
Поточна дата: {today_str}

Завдання: "{instruction_part}"

{metric_hint}

{db_context_text}

ALLOWED_VALUES_JSON (use for exact filters):
{allowed_values_json}

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
   - ⚠️ ВАЖЛИВО: Якщо користувач НЕ вказав конкретну дату чи період, НЕ додавай умову `WHERE date ...`. Аналізуй дані за весь доступний час.

2. ГРУПУВАННЯ ЧАСУ ("потижнево", "weekly", "по місяцях"):
   - Для "потижнево": `GROUP BY DATE_TRUNC(date_column, WEEK)`, у SELECT додай `DATE_TRUNC(date_column, WEEK) AS week_start`.
   - Для "по місяцях": `GROUP BY DATE_TRUNC(date_column, MONTH)`.
   - Обов'язково додай `ORDER BY week_start ASC` (або month_start) для графіків.

3. ФІЛЬТРАЦІЯ ТА ВИКЛЮЧЕННЯ (CRITICAL):
   - Якщо користувач каже "без", "крім", "exclude", "except" (наприклад "без США"), ОБОВ'ЯЗКОВО додай у WHERE умову:
     `AND column != 'value'` або `AND column NOT IN (...)`.
   - Приклад: "Топ 3 країни без США" -> `WHERE geo_country != 'US' ORDER BY revenue DESC LIMIT 3`.
   - **НЕ** додавай фільтри по `app_name`, `platform`, `geo_country`, якщо користувач про це прямо не просив.

4. АНАЛІЗ ВИТРАТ (CRITICAL):
   - **ЗАВЖДИ** використовуй `account_name` для аналізу "видів витрат" або "категорій витрат". Це головне поле!
   - Якщо користувач питає "топ витрат", "структура витрат", "breakdown of costs" - роби `GROUP BY account_name`.
   - Інші важливі поля для аналізу витрат: `subproject_code`, `balance_account_type`.
   - ⚠️ **ВАЖЛИВО:** Використовуй `costrev_center_code` ТІЛЬКИ якщо користувач прямо написав "по центрах витрат", "cost centers" або "costrev_center_code". В інших випадках ігноруй це поле.

5. ТРЕНДИ ТА CTE:
   - Якщо використовуєш WITH (CTE), запит ПОВИНЕН бути завершеним.
   - Обов'язково додай фінальний `SELECT * FROM CTE_NAME` в кінці.
   - НЕ обривай запит на середині.

6. НОВІ / ACQUISITION (Якщо питають "які нові контрагенти/vendors з'явились" або "new revenue"):
   - ⚠️ ОБОВ'ЯЗКОВО включай колонку ДАТИ в SELECT (наприклад `MIN(date)` as `first_seen`), навіть якщо користувач не просив. Це потрібно для аналізу.
   - Використовуй логіку: `SELECT vendor_name, MIN(date) as first_seen FROM ... GROUP BY vendor_name HAVING first_seen >= 'YYYY-01-01'`.
   
7. ЗАГАЛЬНІ:
   - Використовуй ТІЛЬКИ поля зі схеми вище. Не вигадуй нових полів.
   - Якщо запит про "revenue/дохід" — таблиця `{REVENUE_TABLE_REF}`. Якщо "cost/витрати" — `{COST_TABLE_REF}`.
   - Для агрегатів завжди давай alias (наприклад `total_revenue`).
   - Якщо питають "скільки" або "sum" БЕЗ уточнення "по днях/тижнях/категоріях" — НЕ використовуй GROUP BY.
   - Якщо запит про trial / тріали, то використовувати таблицю `{REVENUE_TABLE_REF}` і в ній брати `event_name = "sale"` і `product_id LIKE "%trial%"`.
   - Якщо запит про айді рахунку, або питається про "рахунок", то — використовуй таблицю `{COST_TABLE_REF}` і в ній поле `account_no`.
   - НЕ використовуй SAFE_DIVIDE сам, це зробить авто-корекція. Просто пиши `/`.
   - Поверни ТІЛЬКИ SQL код.
   
8. СПЕЦИФІЧНІ ТЕРМІНИ:
   - Якщо питають "на 1 unit" або "per unit", це означає ділення на кількість унікальних користувачів (COUNT DISTINCT user_id) або кількість продажів (COUNT(*)), залежно від контексту.
   - Не роби фільтр `WHERE unit = 1`, якщо цього поля немає в схемі.
   
9. LTV (Lifetime Value):
   - Якщо запит "LTV когорти" або "найвищий LTV": використовуй SUM(gross_usd) (загальний дохід когорти), якщо не сказано "середній/average".

10. ПОРІВНЯННЯ ТЕКСТУ (CASE INSENSITIVITY):
   - Значення в базі можуть бути в різному регістрі ('Refund', 'refund', 'REFUND') або з пробілами.
   - **ЗАВЖДИ** використовуй `TRIM(LOWER(column)) = 'значення'`, якщо не впевнений.
   - Приклад: `WHERE TRIM(LOWER(event_type)) = 'refund'` (а не `='Refund'`).
   - Дивись на блок "ДОСТУПНІ ЗНАЧЕННЯ В БАЗІ" вище, щоб брати правильні назви (наприклад, 'ADJCOST', 'SALES').

11. КАТЕГОРІАЛЬНІ ФІЛЬТРИ (CRITICAL):
   - Якщо користувач просить фільтр по країні/платформі/додатку/типу івента/провайдеру/юр.особі/центру витрат:
     використовуй ТІЛЬКИ ці колонки:
     • revenue: geo_country, platform, app_name, event_type, provider, revenue_type
     • cost: legal_entity, costrev_center_code, document_type, source_code, account_name, subproject_code, balance_account_type
   - Не вигадуй значення. Бери значення ТІЛЬКИ з ALLOWED_VALUES_JSON.

12. НОРМАЛІЗАЦІЯ ТА МАПІНГ (CRITICAL):
   - Для порівнянь рядків використовуй: TRIM(LOWER(column)) = 'value_lower'
   - Для country codes: використовуй UPPER(TRIM(column)) = 'US' / 'UA' / 'GB'
   - Синоніми країн мап:
     "usa", "united states", "сша" -> 'US'
     "uk", "united kingdom", "британія" -> 'GB'
     "ukraine", "україна" -> 'UA'

13. ЯКЩО КОРИСТУВАЧ НАПИСАВ ЗНАЧЕННЯ, ЯКОГО НЕМАЄ В ALLOWED_VALUES_JSON:
   - НЕ вигадуй "схоже" значення.
   - Замість цього використовуй обережний пошук:
     WHERE TRIM(LOWER(column)) LIKE '%частина_значення%'
     (але не для geo_country — для geo_country тільки точний код).

14. КІЛЬКІСТЬ (COUNT):
   - Якщо користувач питає "скільки чарджбеків/рефандів/підписок" (кількість подій, а не сума грошей):
     використовуй `COUNT(*)` або `COUNT(1)`.
   - НЕ використовуй `SUM(amount)` для кількості.
   - НЕ використовуй `COUNT(DISTINCT ...)`, якщо не просять "унікальних".
   - Кожен рядок в таблиці = 1 подія.

15. EVENT TYPES (CRITICAL):
   - **Допустимі типи:** `sale`, `trial`, `vat`, `wht`, `refund`, `refund_fee`, `chargeback`, `chargeback_fee`, `commission`.
   - ⚠️ НЕ додавай фільтр `event_type`, якщо користувач НЕ просив про конкретний тип.
   - ⚠️ ЯКЩО користувач просить ("subscriptions", "trials", "commissions", "refunds"), ТОДІ додай:
     `TRIM(LOWER(event_type)) = 'sale'` (для subscriptions)
     `TRIM(LOWER(event_type)) = 'trial'`
     `TRIM(LOWER(event_type)) = 'commission'`
     `TRIM(LOWER(event_type)) = 'refund'`
   - Якщо просто "revenue" -> не фільтруй event_type.
   
16. УТОЧНЕННЯ ДЛЯ AI-АНАЛІТИКА (ВАЖЛИВО):
   - Якщо в результатах SQL немає колонки дати (наприклад, через GROUP BY), але користувач просив конкретний рік/період:
     вважай, що дані вже коректно відфільтровані по даті. Не кажи "Даних за цей рік немає", якщо таблиця не порожня.
     
17. ПРІОРИТЕТИ ДЛЯ ВИТРАТ (COST TABLE):
    - Головне поле для групування витрат - `account_name`.
    - Якщо користувач просить "топ-3 статті витрат", це означає `SELECT account_name, SUM(...) ... GROUP BY account_name ORDER BY ... DESC LIMIT 3`.
    - Якщо `account_name` пустий (None) - назви цю групу "Uncategorized" або "Нерозподілені витрати".
"""

    resp = model.generate_content(sql_prompt, generation_config={"temperature": 0})
    sql = resp.text.strip()
    
    # Cleaning
    sql = sql.replace("```sql", "").replace("```", "").strip()
    sql = re.sub(
        r"^\s*(?:```)?\s*(?:bigquery|bigquery\s+sql|BigQuery|BigQuery\s+SQL)\s*[:\-]*\s*",
        "",
        sql,
        flags=re.IGNORECASE | re.MULTILINE,)

    sql = fix_window_order_by(sql)
    sql = _sanitize_sql_dates(sql, date_cols)
    sql = _sanitize_division_by_zero(sql)

    # --- HARDCODED SAFEGUARDS ---
    # 1. Account Number Logic
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

    # 2. Prevent wrong table usage
    if account_no is not None:
        if REVENUE_TABLE_REF in sql:
            raise ValueError("INVALID SQL: revenue table used for account-based cost query")
    
    
    # 3. Cost vs Revenue table check
    if metric in {"cost", "opex", "expense", "expenses"}:
        if REVENUE_TABLE_REF in sql:
            raise ValueError("INVALID SQL: revenue table used for cost metric")


    if account_no is not None:
        sql = _ensure_where_filter(sql, f"account_no = {account_no}")

    # FIX: Sanitize event_type from COST tables (Hallucination fix)
    if COST_TABLE_REF in sql:
        # Removes "WHERE event_type = 'opex'" -> "WHERE 1=1"
        sql = re.sub(r"WHERE\s+event_type\s*=\s*'[^']+'", "WHERE 1=1", sql, flags=re.IGNORECASE)
        # Removes "AND event_type = 'opex'" -> ""
        sql = re.sub(r"\bAND\s+event_type\s*=\s*'[^']+'", "", sql, flags=re.IGNORECASE)

    # ПЕРЕВІРКА: Чи це взагалі SQL? (щоб уникнути помилки \320)
    cleaned_start = sql.strip().upper()
    if not (cleaned_start.startswith("SELECT") or cleaned_start.startswith("WITH")):
        # Кидаємо помилку з текстом відповіді, щоб бот показав її користувачу,
        # замість того, щоб мучити BigQuery.
        raise ValueError(f"🤖 Відповідь AI (не SQL):\n\n{sql}")

    return sql

# ──────────────────────────────────────────────────────────────────────────────
# EXECUTE SINGLE QUERY (INTEGRATED FIX & LOGGING)
# ──────────────────────────────────────────────────────────────────────────────
def execute_single_query(instruction: str, smap: dict, user_id: str = "unknown"):
    logger.info(f"👉 EXECUTE QUERY: {instruction} (User: {user_id})")
    start_time = time.time()
    instruction_part = instruction.strip()
    
    # Змінні для логування
    generated_sql = None
    status = "SUCCESS"
    error_details = None
    final_response = ""
    query_id = None
    
    TOKEN_LIMIT_MSG = (
        "⚠️ **Обмеження контексту**\n"
        "Спробуйте скоротити період або деталізувати запит."
    )

    try:
        if not instruction_part:
            final_response = "Повідомлення порожнє."
            return {"text": final_response, "query_id": None}
            
        if (is_trend_question(instruction_part) and not has_explicit_date(instruction_part)):
            final_response = (
                "❗ Для аналізу динаміки потрібен часовий період.\n\n"
                "Будь ласка, уточніть, наприклад:\n"
                "• за який місяць?\n"
                "• порівняння яких періодів?\n"
                "• конкретний діапазон дат (від–до)"
            )
            return {"text": final_response, "query_id": None}
        
        # >>> MEMORY LOGIC START
        # 1. Завантажуємо повну карту (статика + BQ)
        full_smap = get_semantic_map()

        # 2. Шукаємо точний збіг в пам'яті
        cached_sql = None
        
        try:
             cached_sql = find_exact_match(instruction_part)
        except Exception:
             # logger.warning("Memory read failed (likely permissions), skipping exact match")
             pass
        
        if cached_sql:
            logger.info("⚡ FOUND EXACT MATCH IN MEMORY! Re-using SQL.")
            # --- FIX START: Обробка ситуації, коли пам'ять повертає dict ---
            if isinstance(cached_sql, dict):
                # Спробуємо дістати SQL з різних ймовірних ключів
                generated_sql = cached_sql.get("sql_query") or cached_sql.get("sql") or cached_sql.get("query")
                # Якщо ключів немає, перетворимо на рядок
                if not generated_sql:
                    generated_sql = str(cached_sql)
            else:
                generated_sql = str(cached_sql)
            # --- FIX END ---
        else:
            # 3. Шукаємо схожі запити (RAG)
            memory_context = ""
            try:
                memory_context = find_similar_matches(instruction_part)
            except Exception:
                pass
            
            # 4. Формуємо промпт з контекстом
            # 🔥 FIX: Capture matched fields from Semantic Map to pass as overrides
            matched = find_matches_with_ai(instruction_part, full_smap)
            
            augmented_instruction = instruction_part
            for field, value in matched:
                augmented_instruction += f" ({field}='{value}')"
            
            if memory_context:
                augmented_instruction += f"\n\n[INTERNAL MEMORY - PREVIOUS CORRECT EXAMPLES]:\n{memory_context}\n"

            # 5. Генеруємо SQL
            # 🔥 FIX: Pass matched semantic fields as overrides
            generated_sql = generate_sql(augmented_instruction, full_smap, semantic_overrides=matched)
        # <<< MEMORY LOGIC END

        # === ВИКОНАННЯ ЗАПИТУ ===
        df = execute_cached_query(generated_sql)

        if df.empty:
            # 🔥 FIX: Пояснення, якщо результат порожній і немає дати
            if not has_explicit_date(instruction_part):
                final_response = (
                    "🕵️ **Результат порожній.**\n\n"
                    "Схоже, я не знайшов даних. Це часто буває, коли не вказано часовий період.\n\n"
                    "💡 **Будь ласка, уточніть:**\n"
                    "• Вкажіть дату (наприклад: *'за січень 2023'*, *'за 2024 рік'*)\n"
                    "• Або напишіть *'за весь час'*, якщо хочете загальну статистику."
                )
            else:
                final_response = "Результат порожній (0 рядків). За цей період даних не знайдено."
        else:
            if len(df.columns) == 1 and str(df.columns[0]).startswith("f0_"):
                df = df.rename(columns={df.columns[0]: "value"})

            # === RENDER FUNCTIONS (MOVED UP FOR GLOBAL USE) ===
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

            # === ЛОГІКА ВИЗНАЧЕННЯ ТИПУ ВІДПОВІДІ ===
            is_detailed_transaction_list = False
            col_names = [c.lower() for c in df.columns]
            has_user_id = any(x in col_names for x in ['user_id', 'email', 'account_no', 'customer_id'])
            has_date = any(x in col_names for x in ['date', 'order_date', 'transaction_date', 'event_date', 'posting_date'])
            
            # 1. Список транзакцій (ID + Дата + мало рядків)
            if has_user_id and has_date and len(df) < 30 and len(df) > 0:
                is_detailed_transaction_list = True
            
            # 2. Одна відповідь (1 рядок) — ДЛЯ ТОЧНИХ ЗАПИТІВ (Фікс для "У якому місяці...")
            is_single_row_answer = (len(df) == 1)

            data_for_ai = df.head(50).to_csv(index=False)
            final_display = ""
            analysis_prompt = ""

            # === РОЗГАЛУЖЕННЯ ВІДОБРАЖЕННЯ ===
            if is_detailed_transaction_list:
                # ВАРІАНТ 1: Текстовий список
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
   1. **[Тип транзакції/Статус]** (наприклад: "Початкова підписка", "Продовження", "Refund").
      * **Дата**: YYYY-MM-DD
      * **Сума**: [Сума] [Валюта]
      * **Продукт**: [Назва продукту]
      * **Період**: [Період, якщо є]
      * **Деталі**: (Платформа, Країна, Інше) - що є в даних.
      
3. **Висновки фінансового аналітика** (після списку):
   * **Загалом**: Порахуй суму (LTV) або загальний обсяг.
   * **Поведінка**: Опиши життєвий цикл.
   * **Інше**: Цінова стратегія, географія.

Пиши українською мовою. Використовуй Markdown (bold) для ключів.
"""
            elif is_single_row_answer:
                # ВАРІАНТ 2: Пряма відповідь (Фікс для скріна 2)
                table_md = render_table(df)
                final_display = f"```\n{table_md}\n```\n\n"
                
                analysis_prompt = f"""
Ти — фінансовий аналітик.
SQL запит повернув ОДНЕ значення/рядок. Це і є ПРЯМА ВІДПОВІДЬ.
Не пиши про "недостатньо даних" або "відсутність контексту".

Дані:
{data_for_ai}

Запит: "{instruction_part}"

1. 🎯 **Відповідь**: Чітко сформулюй відповідь на основі значення в таблиці.
2. 💡 **Інсайт** (опціонально): Якщо це метрика (сума, відсоток), дай короткий коментар (багато це чи мало).

Використовуй емоджі та жирний шрифт для акцентів.
"""
            else:
                # ВАРІАНТ 3: Стандартна таблиця + графік (для агрегацій)
                table_md = render_table(df)
                ascii_md = render_ascii_chart(df)
                final_display = f"```\n{table_md}\n```\n{ascii_md}\n\n"

                analysis_prompt = f"""
Ти — старший фінансовий аналітик Headway. Твоє завдання — проаналізувати отримані дані.

Дані (CSV, перші 50 рядків): 
{data_for_ai}

Запит користувача: "{instruction_part}"

ІНСТРУКЦІЇ:
1. Обов'язково розрахуй частки (відсотки) та пропорції, якщо це доречно.
2. Якщо у даних є чітке домінування, обов'язково акцентуй на цьому.
3. Пиши короткими тезами (буллітами).
4. Якщо бачиш аномалії або важливі тренди — виділи їх окремо.
5. Дай інсайт, а не просто переказуй цифри.
4. Оформлення:
   - Використовуй Emoji (🎯 для відповіді, 📊 для деталей, 💡 для інсайтів).
   - Використовуй > Blockquotes для головних висновків.
   - Замість списків точками, розбивай на логічні блочки з жирними заголовками.
"""
            
            # Генерація відповіді AI
            resp = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
            final_response = final_display + resp.text.strip()

    except Exception as e:
        status = "ERROR"
        error_details = str(e)
        logger.error(f"❌ EXECUTION ERROR: {traceback.format_exc()}")
        
        # Обробка лімітів токенів
        if any(k in str(error_details).lower() for k in ["429", "exhausted", "token", "quota"]):
            final_response = (final_display if 'final_display' in locals() else "") + TOKEN_LIMIT_MSG
            status = "TOKEN_LIMIT"
        # Обробка текстових відповідей від AI замість SQL
        elif "🤖 Відповідь AI" in error_details:
            final_response = error_details.replace("ValueError: ", "")
            status = "SUCCESS" 
        else:
            if RETURN_SQL_ON_ERROR and generated_sql:
                final_response = f"❌ SQL ERROR:\n```sql\n{generated_sql}\n```\n{error_details}"
            else:
                final_response = f"❌ Помилка при виконанні SQL:\n{error_details}"

    finally:
        end_time = time.time()
        duration = end_time - start_time
        
        log_interaction(
            user_id=user_id,
            prompt=instruction_part,
            sql=generated_sql,
            response=final_response,
            duration=duration,
            status=status,
            error_msg=error_details
        )
        
        # >>> MEMORY SAVE (SAFE MODE)
        try:
             # Зберігаємо запит в історію, щоб отримати ID для кнопок
             query_id = log_query_to_memory(instruction_part, generated_sql, final_response)
        except Exception as e:
             logger.error(f"Failed to save to memory (likely permissions): {e}")
             query_id = None
        # <<< MEMORY SAVE END

    # Повертаємо структуру, а не просто текст
    return {
        "text": final_response,
        "query_id": query_id
    }

# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINTS (WRAPPED FOR SAFETY)
# ──────────────────────────────────────────────────────────────────────────────
def process_slack_message(message: str, smap: dict, user_id: str = "unknown"):
    try:
        queries = split_into_separate_queries(message)
        # Фільтрація пустих запитів
        queries = [q for q in queries if q.strip() and len(q.strip()) > 2]

        if not queries:
            return {"text": "Не вдалося розпізнати запит.", "query_id": None}

        # Якщо один запит - повертаємо результат як є (dict з query_id)
        if len(queries) == 1:
            return execute_single_query(queries[0], smap, user_id)
            
        # Якщо декілька - комбінуємо текст, але кнопки не додаємо (складно для UI)
        combined_text = f"📝 Знайдено {len(queries)} запитів:\n\n"
        for i, q in enumerate(queries, 1):
            result = execute_single_query(q, smap, user_id)
            ans = result["text"]
            combined_text += f"**Запит {i}:** {q}\n{ans}\n\n"
            
        return {"text": combined_text, "query_id": None}
    except Exception as e:
        logger.error(f"CRITICAL ERROR in process_slack_message: {traceback.format_exc()}")
        return {"text": f"❌ Критична помилка бота: {str(e)}", "query_id": None}

def run_analysis(message: str, semantic_map_override=None, user_id="unknown"):
    try:
        logger.info(f"🚀 RUN ANALYSIS START: {message} (User: {user_id})")
        smap = semantic_map_override or get_semantic_map()
        return process_slack_message(message, smap, user_id)
    except Exception as e:
        logger.error(f"CRITICAL ERROR in run_analysis: {traceback.format_exc()}")
        return {"text": f"❌ Критична помилка запуску: {str(e)}", "query_id": None}
