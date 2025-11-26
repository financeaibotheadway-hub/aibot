from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel
import json
from semantic_map import semantic_map
import pandas as pd
import re
import hashlib
import time
from functools import lru_cache
import os

# ──────────────────────────────────────────────────────────────────────────────
# 1) Конфіг BigQuery (із дефолтами під ваш проект/датасет/таблиці)
# ──────────────────────────────────────────────────────────────────────────────
BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")

REVENUE_TABLE_REF = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_REVENUE_TABLE}"
COST_TABLE_REF    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_COST_TABLE}"

# Ініціалізація клієнтів
bq_client = bigquery.Client(project=BQ_PROJECT)
model = GenerativeModel("gemini-2.5-flash")

# ──────────────────────────────────────────────────────────────────────────────
# 2) Кеш для результатів запитів та схем
# ──────────────────────────────────────────────────────────────────────────────
query_cache = {}
cache_ttl = 300  # 5 хвилин

# кеш схем по кожній таблиці
_schema_cache = {}   # {table_ref: [ {name,type}, ... ]}
_schema_time  = {}   # {table_ref: unix_ts}


def get_cache_key(query: str) -> str:
    return hashlib.md5(query.encode()).hexdigest()


def get_table_schema(table_ref: str, ttl_sec: int = 3600):
    """Кешована схема конкретної таблиці."""
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
    """Повертає обидві схеми для промпта."""
    rev_schema = get_table_schema(REVENUE_TABLE_REF)
    try:
        cost_schema = get_table_schema(COST_TABLE_REF)
    except Exception:
        # якщо таблиці витрат поки немає — працюємо з однією
        cost_schema = []
    return rev_schema, cost_schema


# Попередньо ініціалізуємо (щоб було що підставляти у промпт)
schema_revenue, schema_cost = get_all_schemas()

# ──────────────────────────────────────────────────────────────────────────────
# 3) Кешований виконувач SQL
# ──────────────────────────────────────────────────────────────────────────────
def execute_cached_query(sql_query):
    cache_key = get_cache_key(sql_query)
    now = time.time()

    if cache_key in query_cache:
        cached_df, ts = query_cache[cache_key]
        if now - ts < cache_ttl:
            return cached_df
        else:
            del query_cache[cache_key]

    df = bq_client.query(sql_query).result().to_dataframe()
    query_cache[cache_key] = (df.copy(), now)

    # обмежуємо розмір кешу
    if len(query_cache) > 20:
        oldest_key = min(query_cache, key=lambda k: query_cache[k][1])
        del query_cache[oldest_key]
    return df

# ──────────────────────────────────────────────────────────────────────────────
# 4) Валідатор SQL (легкі перевірки)
# ──────────────────────────────────────────────────────────────────────────────
def validate_sql_syntax(sql_query):
    errors = []

    window_pattern = r'(?:ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\s*\(\s*\)\s+OVER\s*\([^)]*ORDER\s+BY\s+([^)]+)\)'
    window_matches = re.findall(window_pattern, sql_query, re.IGNORECASE)
    for order_expr in window_matches:
        if 'GROUP BY' in sql_query.upper() and not any(
            field in sql_query.split('GROUP BY')[1] for field in order_expr.split(',')
        ):
            errors.append(f"Window ORDER BY містить поле '{order_expr.strip()}', яке не згруповане")

    if re.search(r'WHERE\s+\w+\s+IN\s*\(\s*SELECT.*WHERE.*\w+\.\w+\s*=\s*\w+\.\w+', sql_query,
                 re.IGNORECASE | re.DOTALL):
        errors.append("Використані корельовані підзапити, які не підтримуються BigQuery")

    if 'STRFTIME' in sql_query.upper():
        errors.append("STRFTIME не підтримується в BigQuery. Використовуйте FORMAT_DATE")

    return errors

# ──────────────────────────────────────────────────────────────────────────────
# 5) AI-матчинг семантики (залишив як було)
# ──────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=100)
def find_matches_with_ai_cached(instruction, semantic_map_str):
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
        for pair in result.split(','):
            if ':' in pair:
                field, value = pair.strip().split(':', 1)
                matches.append((field, value))
        return matches
    except:
        return []


def find_matches_with_ai(instruction, smap):
    return find_matches_with_ai_cached(instruction, json.dumps(smap, sort_keys=True))

# ──────────────────────────────────────────────────────────────────────────────
# 6) Розділення складного повідомлення на окремі запити (залишив як було)
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
        for line in result.split('\n'):
            line = line.strip()
            if line.startswith('ЗАПИТ_'):
                parts = line.split(':', 1)
                if len(parts) == 2 and parts[1].strip():
                    queries.append(parts[1].strip())
        return queries if queries else [message]
    except Exception:
        return [message]

# ──────────────────────────────────────────────────────────────────────────────
# 7) Генерація та виконання одного запиту
# ──────────────────────────────────────────────────────────────────────────────
def execute_single_query(instruction: str, smap: dict) -> str:
    try:
        instruction_part = instruction.strip()
        if not instruction_part:
            return "Повідомлення порожнє. Напиши інструкцію."

        matched_conditions = find_matches_with_ai(instruction_part, smap)
        for field, value in matched_conditions:
            instruction_part += f" ({field} = '{value}')"

        # Оновлюємо схеми перед генерацією (раптом оновились)
        rev_schema, cost_schema = get_all_schemas()

        # ───── ПРОМПТ: тепер із двома таблицями ─────
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
- Якщо потрібні window-функції — використовуй коректно з GROUP BY.
- Якщо запит тільки про дохід/продажі — бери дані з таблиці REVENUE.
- Якщо запит тільки про витрати/спенд/кост — бери з таблиці COST.
- Якщо потрібні **ROAS** або **прибуток**, агрегуй REVENUE і COST ОКРЕМО,
  потім **JOIN** за спільними полями (спочатку пробуй date/дата;
  якщо є спільні поля `sourceMedium/source`, `campaign`, `app_name` — додай їх у ключі джойну).
- **ROAS = revenue_value / cost_value**.
- **Профіт/прибуток = revenue_value - cost_value**.
- У таблиці COST вибирай числове поле витрат (перевага назвам: cost, spend, ad_cost, amount, value, usd).
- У таблиці REVENUE для "net revenue"/"нет ревенью" — сумуй **gross_usd** (НЕ фільтруй event_type='sale'; використовуй усі event_type).
- Поле **period** (12M/1M/6M) — це **тип підписки**, не часовий період. Його можна використовувати тільки в GROUP BY для розрізів типів підписки. Не використовуй period у LAG/LEAD/ORDER BY як час.
- Повертай тільки фінальний SQL-запит без пояснень.
"""
        response = model.generate_content(sql_prompt, generation_config={"temperature": 0})
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()
        if sql_query.lower().startswith("sql"):
            sql_query = sql_query[3:].strip()

        errs = validate_sql_syntax(sql_query)
        if errs:
            return "❌ **Помилка в запиті:**\n" + "\n".join(f"• {e}" for e in errs)

        try:
            df = execute_cached_query(sql_query)
        except Exception as bq_error:
            msg = "❌ **Помилка при виконанні запиту до бази даних.**\n"
            if "Window ORDER BY" in str(bq_error):
                msg += "💡 Порада: проблема з window-функцією. Спробуй простіше згортання."
            elif "Correlated subqueries" in str(bq_error):
                msg += "💡 Порада: приберіть корельовані підзапити."
            elif "invalidQuery" in str(bq_error):
                msg += "💡 Порада: синтаксична помилка в SQL."
            return msg

        if df.empty:
            return "Результат таблиці порожній."

        analysis_prompt = f"""
Зроби те, що просить користувач в інструкції.
Інструкція: "{instruction_part}"

CSV результат SQL:
{df.to_csv(index=False)}

Вимоги:
- Не повертай SQL у відповіді.
- Не вигадуй даних або дат — тільки те, що в таблиці.
- Якщо я просив аналітику/пояснення причин — не більше 3–4 речень.
- period (12M/1M/6M) — це типи підписок, не час.
- Якщо є розрізи, можна зазначити: "підписка 12M працює краще ніж 1M".
"""
        analysis_response = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
        return analysis_response.text.strip()

    except Exception as e:
        return f"Помилка під час обробки:\n{str(e)}"

# ──────────────────────────────────────────────────────────────────────────────
# 8) Обробка складних повідомлень і фінальний висновок (без змін по суті)
# ──────────────────────────────────────────────────────────────────────────────
def process_slack_message(message: str, smap: dict) -> str:
    try:
        if not message.strip():
            return "Повідомлення порожнє. Напиши інструкцію."
        queries = split_into_separate_queries(message)
        if len(queries) == 1:
            return execute_single_query(queries[0], smap)

        results = []
        for i, q in enumerate(queries, 1):
            print(f"Виконання запиту {i}/{len(queries)}: {q}")
            results.append((i, q, execute_single_query(q, smap)))

        final = f"📝 **Знайдено {len(queries)} запитів. Відповідаю на кожен:**\n\n"
        for i, q, r in results:
            final += f"**🔍 Запит {i}:** *{q}*\n\n{r}\n\n" + "="*60 + "\n\n"
        return final.rstrip("\n=").rstrip()
    except Exception as e:
        return f"Помилка під час обробки повідомлення:\n{str(e)}"


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

# Утиліти кешу
def clear_cache():
    global query_cache, _schema_cache
    query_cache.clear()
    _schema_cache.clear()
    _schema_time.clear()
    find_matches_with_ai_cached.cache_clear()

def get_cache_stats():
    return {
        "query_cache_size": len(query_cache),
        "ai_cache_info": find_matches_with_ai_cached.cache_info()
    }
