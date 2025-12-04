# analytics.py
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import hashlib
import logging
from functools import lru_cache
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

import vertexai
from vertexai.preview.generative_models import GenerativeModel

from semantic_map import semantic_map  # якщо потрібно використати за замовчуванням

# ──────────────────────────────────────────────────────────────────────────────
# ENV / LOGGING
# ──────────────────────────────────────────────────────────────────────────────
BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")
VERTEX_LOCATION  = os.getenv("VERTEX_LOCATION", "europe-west1")
LOCAL_TZ         = os.getenv("LOCAL_TZ", "Europe/Kyiv")     # TZ для дат

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("ai-bot")

# якщо TRUE — у відповідь у Slack додамо обрізаний SQL і текст помилки
RETURN_SQL_ON_ERROR = os.getenv("RETURN_SQL_ON_ERROR", "false").lower() == "true"

# ──────────────────────────────────────────────────────────────────────────────
# THREADPOOL (гібридний async + threads)
# ──────────────────────────────────────────────────────────────────────────────
MAX_WORKERS = int(os.getenv("AI_BOT_WORKERS", "4"))
_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


async def _run_in_executor(func, *args, **kwargs):
    """Універсальна обгортка: запуск будь-якої sync-функції в threadpool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_executor, lambda: func(*args, **kwargs))


# ──────────────────────────────────────────────────────────────────────────────
# INIT CLIENTS
# ──────────────────────────────────────────────────────────────────────────────
REVENUE_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_REVENUE_TABLE}"
COST_TABLE_REF    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_COST_TABLE}"

# BigQuery
bq_client = bigquery.Client(project=BQ_PROJECT)

# Vertex AI
try:
    vertexai.init(project=BQ_PROJECT, location=VERTEX_LOCATION)
except Exception:
    logger.warning("Vertex init failed; will rely on ambient creds", exc_info=True)
model = GenerativeModel("gemini-2.5-flash")

# ──────────────────────────────────────────────────────────────────────────────
# CACHE (SQL + schemas)
# ──────────────────────────────────────────────────────────────────────────────
query_cache = {}  # key -> (df, ts)
cache_ttl = 300   # seconds

_schema_cache = {}  # table_ref -> [{"name": ..., "type": ...}]
_schema_time  = {}  # table_ref -> ts


def get_cache_key(query: str) -> str:
    return hashlib.md5(query.encode("utf-8")).hexdigest()


def get_table_schema(table_ref: str, ttl_sec: int = 3600):
    """Return cached schema for table."""
    now = time.time()
    if (
        table_ref not in _schema_cache
        or table_ref not in _schema_time
        or now - _schema_time[table_ref] > ttl_sec
    ):
        schema = bq_client.get_table(table_ref).schema
        _schema_cache[table_ref] = [{"name": f.name, "type": f.field_type} for f in schema]
        _schema_time[table_ref] = now
    return _schema_cache[table_ref]


def get_all_schemas():
    rev_schema = get_table_schema(REVENUE_TABLE_REF)
    try:
        cost_schema = get_table_schema(COST_TABLE_REF)
    except Exception:
        cost_schema = []
    return rev_schema, cost_schema


# попередньо ініціалізуй (корисно для першого промпта)
_ = get_all_schemas()

# ──────────────────────────────────────────────────────────────────────────────
# УТИЛІТИ ДАТ / SQL / CSV
# ──────────────────────────────────────────────────────────────────────────────
def _collect_date_columns(schema_list):
    """Повертає множину полів, які мають DATE/DATETIME/TIMESTAMP (щоб їх не парсили як STRING)."""
    return {
        f["name"]
        for f in schema_list
        if f.get("type") in ("DATE", "DATETIME", "TIMESTAMP")
    }


def _sanitize_sql_dates(sql_query: str, date_columns: set) -> str:
    """
    Пост-обробка SQL: прибирає PARSE_DATE(..., <date_col>) та SAFE.PARSE_DATE для відомих DATE-полів,
    підставляє CURRENT_DATE('<tz>') якщо без TZ.
    """
    original = sql_query
    upper = sql_query.upper()

    # Якщо немає ні CURRENT_DATE, ні PARSE_DATE — нічого не робимо
    if "CURRENT_DATE" not in upper and "PARSE_DATE" not in upper and "SAFE.PARSE_DATE" not in upper:
        return sql_query

    # 1) CURRENT_DATE() / CURRENT_DATE  → CURRENT_DATE('Europe/Kyiv')
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

    # 2) Для кожного відомого DATE-поля прибираємо PARSE_DATE('%Y-%m-%d', col) -> col
    for col in sorted(date_columns, key=len, reverse=True):
        # з іменами-аліасами типу t.posting_date або `posting_date`
        pattern_plain = rf"PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"

        def _repl_plain(m):
            inner = m.group(1)
            inner_clean = inner.strip("`")
            if inner_clean.endswith(f".{col}") or inner_clean == col:
                return inner
            return m.group(0)

        sql_query = re.sub(pattern_plain, _repl_plain, sql_query, flags=re.IGNORECASE)

        # SAFE.PARSE_DATE(...) -> CAST(col AS DATE)
        pattern_safe = rf"SAFE\.PARSE_DATE\(\s*'[^']+'\s*,\s*(`?[\w\.]+`?)\s*\)"

        def _repl_safe(m):
            inner = m.group(1)
            inner_clean = inner.strip("`")
            if inner_clean.endswith(f".{col}") or inner_clean == col:
                return f"CAST({inner} AS DATE)"
            return m.group(0)

        sql_query = re.sub(pattern_safe, _repl_safe, sql_query, flags=re.IGNORECASE)

    if sql_query != original:
        logger.info("[sanitize] SQL was sanitized for date handling")

    return sql_query


def normalize_sql(sql: str) -> str:
    """Нормалізація SQL для кешу (щоб дрібні відмінності не ламали cache)."""
    sql = sql.strip()
    sql = re.sub(r"\s+", " ", sql)
    sql = sql.lower()
    return sql


def limited_csv(df: pd.DataFrame, max_rows: int = 7) -> str:
    """Даємо Vertex тільки невеликий фрагмент результату для аналізу."""
    if df.empty:
        return "EMPTY_RESULT"

    if len(df) <= max_rows:
        return df.to_csv(index=False)

    # head + tail
    half = max_rows // 2
    head = df.head(half)
    tail = df.tail(max_rows - half)

    txt = "HEAD:\n" + head.to_csv(index=False)
    txt += "\nTAIL:\n" + tail.to_csv(index=False)
    txt += f"\n[TRUNCATED] total_rows={len(df)}"
    return txt


def auto_fix_group_by(sql: str) -> str:
    """Автоматичний фікс помилки 'column X which is neither grouped nor aggregated'."""
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

            # ігноруємо агрегати
            if re.search(r"(sum|count|min|max|avg)\s*\(", clean, re.IGNORECASE):
                continue

            # видаляємо alias
            clean_no_alias = re.sub(r"\s+as\s+.*", "", clean, flags=re.IGNORECASE).strip()

            # ігноруємо літерали
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


# ──────────────────────────────────────────────────────────────────────────────
# EXTRA SQL FIXES (FORMAT_DATE + зайві коми)
# ──────────────────────────────────────────────────────────────────────────────
def fix_format_date(sql: str) -> str:
    """Лікує типові помилки з FORMAT_DATE."""

    # FORMAT_DATE('%Y-%m')  → вважаємо, що потрібно за posting_date
    sql = re.sub(
        r"FORMAT_DATE\s*\(\s*'([^']+)'\s*\)",
        r"FORMAT_DATE('\1', posting_date)",
        sql,
        flags=re.IGNORECASE,
    )

    # FORMAT_DATE('%Y-%m', posting_date,) → прибрати зайву кому
    sql = re.sub(
        r"FORMAT_DATE\s*\(([^,]+),\s*([^\)]+),\s*\)",
        r"FORMAT_DATE(\1, \2)",
        sql,
        flags=re.IGNORECASE,
    )

    # FORMAT_DATE('%Y-%m' posting_date) → вставити кому
    sql = re.sub(
        r"FORMAT_DATE\s*\(\s*'([^']+)'\s+(\w+)\s*\)",
        r"FORMAT_DATE('\1', \2)",
        sql,
        flags=re.IGNORECASE,
    )

    # FORMAT_DATE(...) без alias'а → додаємо AS month
    sql = re.sub(
        r"(FORMAT_DATE\s*\([^\)]+\))(?!\s+as|\s*,)",
        r"\1 AS month",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def fix_trailing_commas(sql: str) -> str:
    """Прибирає зайві коми перед FROM/WHERE/GROUP BY/ORDER BY і перед закриваючою дужкою."""
    # кома перед ключовими словами
    sql = re.sub(r",\s*(FROM|WHERE|GROUP BY|ORDER BY)", r" \1", sql, flags=re.IGNORECASE)
    # кома перед переведенням рядка + ключовим словом
    sql = re.sub(r",\s*\n\s*(FROM|WHERE|GROUP BY|ORDER BY)", r"\n\1", sql, flags=re.IGNORECASE)
    # подвійні коми
    sql = re.sub(r",\s*,", ", ", sql)
    # кома перед закриваючою дужкою
    sql = re.sub(r",\s*\)", ")", sql)
    return sql


def full_sql_fix(sql: str, date_cols: set) -> str:
    """Єдиний pipeline для ремонту SQL."""
    sql_before = sql
    sql = _sanitize_sql_dates(sql, date_cols)
    sql = fix_format_date(sql)
    sql = fix_trailing_commas(sql)
    sql = auto_fix_group_by(sql)

    if sql != sql_before:
        logger.info("[sql fix] SQL was post-processed by full_sql_fix")

    return sql


# ──────────────────────────────────────────────────────────────────────────────
# BQ EXECUTOR (with logging)
# ──────────────────────────────────────────────────────────────────────────────
def execute_cached_query(sql_query: str):
    # нормалізований SQL як ключ кешу
    cache_key = get_cache_key(normalize_sql(sql_query))
    now = time.time()

    # cache HIT
    if cache_key in query_cache:
        df, ts = query_cache[cache_key]
        if now - ts < cache_ttl:
            logger.info(
                "[bq] cache HIT key=%s age=%.1fs rows=%d",
                cache_key[:8],
                now - ts,
                len(df),
            )
            return df

    # cache MISS
    logger.info("[bq] cache MISS key=%s", cache_key[:8])
    start = time.perf_counter()
    job = bq_client.query(sql_query)

    try:
        df = job.result().to_dataframe()
        took = time.perf_counter() - start
        logger.info("[bq] OK job_id=%s rows=%d time=%.3fs", job.job_id, len(df), took)

        query_cache[cache_key] = (df.copy(), now)
        # trim cache
        if len(query_cache) > 20:
            oldest_key = min(query_cache, key=lambda k: query_cache[k][1])
            del query_cache[oldest_key]
        return df

    except BadRequest as e:
        msg = getattr(e, "message", str(e))
        logger.exception("[bq] BadRequest job_id=%s : %s", getattr(job, "job_id", "?"), msg)
        raise
    except Exception:
        logger.exception("[bq] FAILED job_id=%s", getattr(job, "job_id", "?"))
        raise


# ──────────────────────────────────────────────────────────────────────────────
# SQL SYNTAX VALIDATION (light checks)
# ──────────────────────────────────────────────────────────────────────────────
def validate_sql_syntax(sql_query: str):
    errors = []

    window_pattern = r"(?:ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\s*\(\s*\)\s+OVER\s*\([^)]*ORDER\s+BY\s+([^)]+)\)"
    window_matches = re.findall(window_pattern, sql_query, re.IGNORECASE)
    for order_expr in window_matches:
        if "GROUP BY" in sql_query.upper() and not any(
            field in sql_query.split("GROUP BY")[1] for field in order_expr.split(",")
        ):
            errors.append(f"Window ORDER BY містить поле '{order_expr.strip()}', яке не згруповане")

    if re.search(
        r"WHERE\s+\w+\s+IN\s*\(\s*SELECT.*WHERE.*\w+\.\w+\s*=\s*\w+\.\w+",
        sql_query,
        re.IGNORECASE | re.DOTALL,
    ):
        errors.append("Використані корельовані підзапити, які не підтримуються BigQuery")

    if "STRFTIME" in sql_query.upper():
        errors.append("STRFTIME не підтримується в BigQuery. Використовуйте FORMAT_DATE")

    return errors


# ──────────────────────────────────────────────────────────────────────────────
# AI matching
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=100)
def find_matches_with_ai_cached(instruction: str, semantic_map_str: str):
    smap = json.loads(semantic_map_str)

    context = {}
    for full_key, phrases in smap.items():
        field, value = full_key.split(":")
        context.setdefault(field, {})
        synonyms = []
        for p in phrases:
            synonyms.append(p.get("text", "") if isinstance(p, dict) else str(p))
        context[field][value] = synonyms

    prompt = f"""
Знайди які поля згадує користувач, використовуючи синоніми:

Доступні поля та синоніми:
{json.dumps(context, ensure_ascii=False, indent=2)}

Текст користувача: "{instruction}"

Правила:
- Якщо "фі" + "рефанд" → event_type=refund_fee
- Якщо тільки "рефанд" → event_type=refund
- Якщо "фі" + "чарджбек" → event_type=chargeback_fee
- Якщо тільки "чарджбек" → event_type=chargeback
"""
    try:
        response = model.generate_content(prompt, generation_config={"temperature": 0})
        result = response.text.strip()
        if result == "NONE":
            return []
        matches = []
        for pair in result.split(","):
            if ":" in pair:
                field, value = pair.strip().split(":", 1)
                matches.append((field, value))
        return matches
    except Exception:
        return []


def find_matches_with_ai(instruction, smap):
    return find_matches_with_ai_cached(instruction, json.dumps(smap, sort_keys=True))


# ──────────────────────────────────────────────────────────────────────────────
# Split complex message
# ──────────────────────────────────────────────────────────────────────────────
def split_into_separate_queries(message: str) -> list:
    split_prompt = f"""
Розділи повідомлення користувача на окремі незалежні запити. Кожне питання або завдання має бути окремим запитом.

Повідомлення: "{message}"

Знайди всі окремі питання/завдання та перелічи їх в такому форматі:
ЗАПИТ_1: [перший запит]
ЗАПИТ_2: [другий запит]
ЗАПИТ_3: [третій запит]
"""
    try:
        response = model.generate_content(split_prompt, generation_config={"temperature": 0})
        result = response.text.strip()
        queries = []
        for line in result.split("\n"):
            line = line.strip()
            if line.startswith("ЗАПИТ_"):
                parts = line.split(":", 1)
                if len(parts) == 2 and parts[1].strip():
                    queries.append(parts[1].strip())
        return queries if queries else [message]
    except Exception:
        return [message]


# ──────────────────────────────────────────────────────────────────────────────
# Main executors (sync)
# ──────────────────────────────────────────────────────────────────────────────
def execute_single_query(instruction: str, smap: dict, user_id: str = "unknown") -> str:
    try:
        instruction_part = instruction.strip()
        if not instruction_part:
            return "Повідомлення порожнє. Напиши інструкцію."

        logger.info("[execute_single_query] user_id=%s instruction=%s", user_id, instruction_part)

        matched_conditions = find_matches_with_ai(instruction_part, smap)
        for field, value in matched_conditions:
            instruction_part += f" ({field} = '{value}')"
        if matched_conditions:
            logger.debug("[execute_single_query] matched_conditions=%s", matched_conditions)

        rev_schema, cost_schema = get_all_schemas()

        # зберемо відомі DATE-поля (щоб не парсити їх як STRING)
        date_cols = _collect_date_columns(rev_schema) | _collect_date_columns(cost_schema)
        date_cols_list = sorted(list(date_cols))

        sql_prompt = f"""
В нас є ДВІ таблиці в BigQuery:

1) **REVENUE**: `{REVENUE_TABLE_REF}`
   Схема:
{json.dumps(rev_schema, indent=2)}

2) **COST**: `{COST_TABLE_REF}`
   Схема:
{json.dumps(cost_schema, indent=2)}

Згенеруй ЕКСПЕРТНИЙ BigQuery SQL для завдання: {instruction_part}

Правила:
- Використовуй ТІЛЬКИ BigQuery SQL.
- Не використовуй STRFTIME; для форматів дат: FORMAT_DATE('%Y-%m', DATE(...)).
- Не використовуй корельовані підзапити.
- Якщо запит тільки про дохід/продажі — REVENUE.
- Якщо тільки про витрати — COST.
- Для ROAS/прибутку — агрегуй окремо та JOIN.
- У REVENUE для «net revenue» — сумуй gross_usd (усі event_type).
- period (12M/1M/6M) — це тип підписки, не час.
- **Часовий пояс** для відносних дат: CURRENT_DATE('{LOCAL_TZ}').
- **Важливо**: поля типу DATE/DATETIME/TIMESTAMP не треба парсити як STRING.
  У цих таблицях поля дат: {date_cols_list}. Порівнюй їх без PARSE_DATE.
  Наприклад, "вчора": posting_date = DATE_SUB(CURRENT_DATE('{LOCAL_TZ}'), INTERVAL 1 DAY).
- Поверни лише фінальний SQL без пояснень.
"""
        response = model.generate_content(sql_prompt, generation_config={"temperature": 0})
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()
        if sql_query.lower().startswith("sql"):
            sql_query = sql_query[3:].strip()

        logger.info("[sql raw] %s", sql_query)

        # пост-обробка SQL (дати + FORMAT_DATE + коми + GROUP BY)
        sql_query = full_sql_fix(sql_query, date_cols)
        logger.info("[sql fixed] %s", sql_query)

        errs = validate_sql_syntax(sql_query)
        logger.debug("[execute_single_query] generated SQL:\n%s", sql_query)
        if errs:
            logger.warning("[execute_single_query] validation errors: %s", errs)
            return "❌ **Помилка в запиті:**\n" + "\n".join(f"• {e}" for e in errs)

        try:
            df = execute_cached_query(sql_query)
        except BadRequest as e:
            msg = getattr(e, "message", str(e))[:600]
            out = "❌ **Помилка при виконанні запиту до бази даних.**\n"
            if RETURN_SQL_ON_ERROR:
                out += f"SQL:\n```sql\n{sql_query[:1500]}\n```\n"
            out += f"Помилка BigQuery:\n```\n{msg}\n```"
            return out
        except Exception as e:
            msg = (getattr(e, "message", None) or str(e))[:600]
            logger.exception("[execute_single_query] unexpected error")
            out = "❌ **Помилка при виконанні запиту до бази даних.**\n"
            if RETURN_SQL_ON_ERROR:
                out += f"SQL:\n```sql\n{sql_query[:1500]}\n```\n"
            out += f"Деталі:\n```\n{msg}\n```"
            return out

        if df.empty:
            logger.info("[execute_single_query] empty result")
            return "Результат таблиці порожній."

        analysis_prompt = f"""
Зроби те, що просить користувач в інструкції.
Інструкція: "{instruction_part}"

CSV результат SQL (урізаний до важливого):
{limited_csv(df)}

Вимоги:
- Не повертай SQL у відповіді.
- Не вигадуй даних або дат — тільки те, що в таблиці.
- Якщо просили аналіз — до 3–4 речень.
- period (12M/1M/6M) — це типи підписок, не час.
"""
        analysis_response = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
        return analysis_response.text.strip()

    except Exception as e:
        logger.exception("[execute_single_query] fatal")
        return "Помилка під час обробки:\n" + (getattr(e, "message", None) or str(e))


def process_slack_message(message: str, smap: dict, user_id: str = "unknown") -> str:
    try:
        msg = message or ""
        if not msg.strip():
            return "Повідомлення порожнє. Напиши інструкцію."

        queries = split_into_separate_queries(msg)
        if len(queries) == 1:
            return execute_single_query(queries[0], smap, user_id=user_id)

        results = []
        for i, q in enumerate(queries, 1):
            try:
                logger.info(
                    "[process_slack_message] user_id=%s part=%d/%d: %s",
                    user_id, i, len(queries), q
                )
                ans = execute_single_query(q, smap, user_id=user_id)
            except Exception:
                logger.exception("[process_slack_message] failed on part %d", i)
                ans = "❌ Помилка під час обробки цього окремого запиту."
            results.append((i, q, ans))

        final = f"📝 **Знайдено {len(queries)} запитів. Відповідаю на кожен:**\n\n"
        for i, q, r in results:
            final += f"**🔍 Запит {i}:** *{q}*\n\n{r}\n\n" + "=" * 60 + "\n\n"
        return final.rstrip("\n=").rstrip()
    except Exception:
        logger.exception("[process_slack_message] fatal")
        return "Помилка під час обробки повідомлення."


def generate_final_conclusion(results: list, original_message: str) -> str:
    try:
        conclusions = []
        for i, q, r in results:
            if "Висновок:" in r:
                conclusions.append(f"Запит {i}: {r.split('Висновок:')[-1].strip()}")
        if not conclusions:
            return ""
        summary_prompt = f"""
На основі результатів всіх запитів дай один загальний висновок.

Оригінальне повідомлення: "{original_message}"
Результати:
{chr(10).join(conclusions)}

Сформуй короткий підсумок (2–4 речення).
"""
        response = model.generate_content(summary_prompt, generation_config={"temperature": 0})
        return f"📋 **ЗАГАЛЬНИЙ ВИСНОВОК:**\n{response.text.strip()}"
    except Exception:
        return f"📋 **ЗАГАЛЬНИЙ ВИСНОВОК:**\nВсі запити оброблено успішно."


# ──────────────────────────────────────────────────────────────────────────────
# ASYNC-версії API (для FastAPI / Slack handler)
# ──────────────────────────────────────────────────────────────────────────────
async def execute_single_query_async(instruction: str, smap: dict, user_id: str = "unknown") -> str:
    """Async-обгортка для execute_single_query."""
    return await _run_in_executor(execute_single_query, instruction, smap, user_id)


async def process_slack_message_async(message: str, smap: dict, user_id: str = "unknown") -> str:
    """Async-обгортка для process_slack_message."""
    return await _run_in_executor(process_slack_message, message, smap, user_id)


# Utils
def clear_cache():
    global query_cache, _schema_cache
    query_cache.clear()
    _schema_cache.clear()
    _schema_time.clear()
    find_matches_with_ai_cached.cache_clear()


def get_cache_stats():
    return {
        "query_cache_size": len(query_cache),
        "ai_cache_info": find_matches_with_ai_cached.cache_info(),
    }
