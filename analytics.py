from google.cloud import bigquery
from vertexai.preview.generative_models import GenerativeModel
import json
from semantic_map import semantic_map
import pandas as pd
import re
import hashlib
import time
from functools import lru_cache

# Ініціалізація клієнтів
bq_client = bigquery.Client()
model = GenerativeModel("gemini-2.5-flash")

# Параметри таблиці
project_id = "thermal-beach-465608-h7"
dataset_id = "test_vertex_ai"
table_id = "Revenue"
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Кеш для результатів запитів
query_cache = {}
cache_ttl = 300  # 5 хвилин

# Кеш схеми
_schema_cache = None
_schema_time = 0


def get_cache_key(query: str) -> str:
    return hashlib.md5(query.encode()).hexdigest()


# Кешована схема таблиці
def get_table_schema():
    global _schema_cache, _schema_time
    current_time = time.time()

    if _schema_cache is None or current_time - _schema_time > 3600:  # 1 година
        schema = bq_client.get_table(table_ref).schema
        _schema_cache = [{"name": f.name, "type": f.field_type} for f in schema]
        _schema_time = current_time

    return _schema_cache


schema_info = get_table_schema()


# Кешований запит
def execute_cached_query(sql_query):
    cache_key = get_cache_key(sql_query)
    current_time = time.time()

    if cache_key in query_cache:
        cached_data, timestamp = query_cache[cache_key]
        if current_time - timestamp < cache_ttl:
            return cached_data
        else:
            del query_cache[cache_key]

    query_job = bq_client.query(sql_query)
    df = query_job.result().to_dataframe()

    query_cache[cache_key] = (df.copy(), current_time)

    # Обмежуємо кеш
    if len(query_cache) > 20:
        oldest_key = min(query_cache.keys(), key=lambda k: query_cache[k][1])
        del query_cache[oldest_key]

    return df


# Валідація SQL перед виконанням
def validate_sql_syntax(sql_query):
    """Швидка перевірка на типові помилки BigQuery"""
    errors = []

    # Перевірка на window functions з неправильним ORDER BY
    window_pattern = r'(?:ROW_NUMBER|RANK|DENSE_RANK|LAG|LEAD)\s*\(\s*\)\s+OVER\s*\([^)]*ORDER\s+BY\s+([^)]+)\)'
    window_matches = re.findall(window_pattern, sql_query, re.IGNORECASE)

    for order_expr in window_matches:
        # Якщо ORDER BY містить поле, що не згруповане
        if 'GROUP BY' in sql_query.upper() and not any(
                field in sql_query.split('GROUP BY')[1] for field in order_expr.split(',')):
            errors.append(f"Window ORDER BY містить поле '{order_expr.strip()}', яке не згруповане")

    # Перевірка на correlated subqueries
    if re.search(r'WHERE\s+\w+\s+IN\s*\(\s*SELECT.*WHERE.*\w+\.\w+\s*=\s*\w+\.\w+', sql_query,
                 re.IGNORECASE | re.DOTALL):
        errors.append("Використані корельовані підзапити, які не підтримуються BigQuery")

    # Перевірка на використання STRFTIME
    if 'STRFTIME' in sql_query.upper():
        errors.append("STRFTIME не підтримується в BigQuery. Використовуйте FORMAT_DATE")

    return errors


# AI замість токенізації
@lru_cache(maxsize=100)
def find_matches_with_ai_cached(instruction, semantic_map_str):
    semantic_map = json.loads(semantic_map_str)

    # Конвертуємо semantic_map в зрозумілий формат для AI
    context = {}
    for full_key, phrases in semantic_map.items():
        field, value = full_key.split(":")
        if field not in context:
            context[field] = {}

        synonyms = []
        for p in phrases:
            if isinstance(p, dict):
                synonyms.append(p.get("text", ""))
            else:
                synonyms.append(str(p))
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


def find_matches_with_ai(instruction, semantic_map):
    semantic_map_str = json.dumps(semantic_map, sort_keys=True)
    return find_matches_with_ai_cached(instruction, semantic_map_str)


# НОВА ФУНКЦІЯ: Розділення на окремі запити
def split_into_separate_queries(message: str) -> list:
    """Розділяє повідомлення на окремі запити"""

    split_prompt = f"""
Розділи повідомлення користувача на окремі незалежні запити. Кожне питання або завдання має бути окремим запитом.

Повідомлення: "{message}"

Знайди всі окремі питання/завдання та перелічи їх в такому форматі:
ЗАПИТ_1: [перший запит]
ЗАПИТ_2: [другий запит]
ЗАПИТ_3: [третій запит]
... і так далі

Правила:
- Якщо в повідомленні тільки одне питання/завдання, поверни тільки ЗАПИТ_1
- Кожний запит повинен бути повним та зрозумілим сам по собі
- Не додавай запити, яких немає в оригінальному повідомленні
- Зберігай оригінальний сенс кожного запиту

Приклади:
"Скільки було продажів у січні? А скільки рефандів?" →
ЗАПИТ_1: Скільки було продажів у січні?
ЗАПИТ_2: Скільки було рефандів у січні?

"Показати топ країн за доходом" →
ЗАПИТ_1: Показати топ країн за доходом
"""

    try:
        response = model.generate_content(split_prompt, generation_config={"temperature": 0})
        result = response.text.strip()

        # Парсимо запити
        queries = []
        lines = result.split('\n')

        for line in lines:
            line = line.strip()
            if line.startswith('ЗАПИТ_'):
                parts = line.split(':', 1)
                if len(parts) == 2:
                    query = parts[1].strip()
                    if query:
                        queries.append(query)

        return queries if queries else [message]

    except Exception as e:
        print(f"Помилка при розділенні на запити: {e}")
        return [message]


# НОВА ФУНКЦІЯ: Виконання одного простого запиту
def execute_single_query(instruction: str, semantic_map: dict) -> str:
    """Виконує один простий запит"""

    try:
        instruction_part = instruction.strip()
        if not instruction_part:
            return "Повідомлення порожнє. Напиши інструкцію."

        # AI знаходить співпадіння замість складного алгоритму
        matched_conditions = find_matches_with_ai(instruction_part, semantic_map)

        for field, value in matched_conditions:
            instruction_part += f" ({field} = '{value}')"

        sql_prompt = f"""
Схема таблиці:
{json.dumps(schema_info, indent=2)}

Згенеруй ЕКСПЕРТНИЙ BigQuery SQL-запит для: {instruction_part}

Основні правила:
- використовуй тільки BigQuery SQL;
- не використовуй STRFTIME, замість цього FORMAT_DATE('%Y-%m', DATE(...));
- НІКОЛИ не використовуй корельовані підзапити (correlated subqueries);
- Якщо потрібні window functions, використовуй їх правильно з GROUP BY;
- назви таблиць та полів бери зі схеми;
- назву таблиці вкажи як {table_ref};
- якщо в інструкції присутній текст '(field = "...")', використовуй це як частину WHERE;
- поверни тільки SQL-запит;
- "опис" = "description";
- коли кажуть "net revenue" чи "нет ревенью" в реченні(нет і ревенью можуть бути не поруч одне з одним), сумуй весь стовпець gross_usd і не використовуй event_type = "sale", використовуй усі значення у стовпці event_type;
- Використовуй revenue_type = 'New' тільки якщо в інструкції явно згадано слово "новий" або "нью". Якщо йдеться про дохід чи про ревенью загалом, не додавай цей фільтр.

СТРАТЕГІЯ АНАЛІЗУ:
Коли потрібно пояснити зміни в даних або зрозуміти що відбувається - завжди аналізуй app_name, event_type, revenue_type, period (як окремо, так і в комбінаціях). Саме вони дають відповіді на питання про причини подій.

Експертні техніки:
- Використовуй CTEs (WITH) для складних запитів
- APPROX_QUANTILES для процентілів
- SAFE_DIVIDE для безпечного ділення
- ROW_NUMBER() OVER для рангування
- LAG/LEAD для порівнянь з попередніми періодами


🚨 КРИТИЧНО ВАЖЛИВО - ПОЛЕ period:
- period містить значення типу: 12M, 1M, 6M, 3M тощо
- ЦЕ НЕ ЧАСОВІ ПЕРІОДИ! Це коди типів підписки!
- 12M = тип підписки "12-місячна", 1M = тип підписки "місячна"
- Використовуй period ТІЛЬКИ для GROUP BY по типах підписки
- НІКОЛИ не використовуй period в ORDER BY для часових трендів
- НІКОЛИ не використовуй period в LAG/LEAD функціях
        """

        response = model.generate_content(sql_prompt, generation_config={"temperature": 0})
        sql_query = response.text.strip().replace("```sql", "").replace("```", "").strip()
        if sql_query.lower().startswith("sql"):
            sql_query = sql_query[3:].strip()

        # Швидка перевірка SQL перед виконанням
        validation_errors = validate_sql_syntax(sql_query)
        if validation_errors:
            error_msg = "❌ **Помилка в запиті:**\n"
            for error in validation_errors:
                error_msg += f"• {error}\n"
            return error_msg

        try:
            df = execute_cached_query(sql_query)
        except Exception as bq_error:
            error_msg = "❌ **Помилка при виконанні запиту до бази даних.**\n"

            # Додаткові поради для типових помилок
            if "Window ORDER BY" in str(bq_error):
                error_msg += "💡 **Порада:** Проблема з window функцією. Перефразуйте запит простіше."
            elif "Correlated subqueries" in str(bq_error):
                error_msg += "💡 **Порада:** Спробуйте перефразувати запит без складних підзапитів."
            elif "invalidQuery" in str(bq_error):
                error_msg += "💡 **Порада:** Синтаксична помилка в SQL. Спробуйте простіший запит."

            return error_msg

        if df.empty:
            return "Результат таблиці порожній."

        analysis_prompt = f"""
Зроби те, що просить тебе зробити користувач в інструкції.
Інструкція користувача:
"{instruction_part}"
CSV-таблиця результату SQL-запиту:

{df.to_csv(index=False)}

Обов'язково: 
- Не повертай дані у форматі SQL.
- Не пиши вступ 'з CSV таблиці...".
- Не вигадуй дані або дати, яких немає в таблиці.
- Дай короткий опис того, що ти шукав, не придумай того, чого насправді не було в промпті чи в даних.
- Якщо я прошу зробити аналітику або пояснити причини та наслідки, скорочуй пояснювальну або аналітичну частину відповіді до максимум 3–4 речень.
- period (12M, 1M, 6M) - це типи підписок, НЕ часові періоди
- При аналізі period показуй: "підписка 12M працює краще ніж 1M"
- НЕ говори про "тренди по періодах" - говори про "порівняння типів підписок"
        """

        analysis_response = model.generate_content(analysis_prompt, generation_config={"temperature": 0})
        return analysis_response.text.strip()

    except Exception as e:
        return f"Помилка під час обробки:\n{str(e)}"


# ОСНОВНА ФУНКЦІЯ: Обробка Slack-повідомлення з розділенням на запити
def process_slack_message(message: str, semantic_map: dict) -> str:
    """Головна функція обробки повідомлень"""

    try:
        if not message.strip():
            return "Повідомлення порожнє. Напиши інструкцію."

        # Розділяємо на окремі запити
        queries = split_into_separate_queries(message)

        # Якщо тільки один запит - виконуємо як зазвичай
        if len(queries) == 1:
            return execute_single_query(queries[0], semantic_map)

        # Якщо кілька запитів - виконуємо кожен окремо
        results = []

        for i, query in enumerate(queries, 1):
            print(f"Виконання запиту {i}/{len(queries)}: {query}")

            result = execute_single_query(query, semantic_map)
            results.append((i, query, result))

        # Формуємо фінальну відповідь
        final_response = f"📝 **Знайдено {len(queries)} запитів. Відповідаю на кожен:**\n\n"

        for i, query, result in results:
            final_response += f"**🔍 Запит {i}:** *{query}*\n\n{result}\n\n"
            final_response += "=" * 60 + "\n\n"

        return final_response.rstrip("\n=").rstrip()

    except Exception as e:
        return f"Помилка під час обробки повідомлення:\n{str(e)}"


# ДОДАТКОВА ФУНКЦІЯ: Генерація фінального висновку
def generate_final_conclusion(results: list, original_message: str) -> str:
    """Генерує загальний висновок на основі всіх результатів"""

    try:
        # Збираємо всі висновки з результатів
        conclusions = []
        for i, query, result in results:
            if "Висновок:" in result:
                conclusion = result.split("Висновок:")[-1].strip()
                conclusions.append(f"Запит {i}: {conclusion}")

        if not conclusions:
            return ""

        summary_prompt = f"""
На основі результатів всіх запитів дай один загальний висновок.

Оригінальне повідомлення користувача: "{original_message}"

Результати запитів:
{chr(10).join(conclusions)}

Створи один короткий загальний висновок (2-4 речення), який підсумовує всі отримані дані та відповідає на оригінальне питання користувача:
"""

        response = model.generate_content(summary_prompt, generation_config={"temperature": 0})
        return f"📋 **ЗАГАЛЬНИЙ ВИСНОВОК:**\n{response.text.strip()}"

    except Exception as e:
        return f"📋 **ЗАГАЛЬНИЙ ВИСНОВОК:**\nВсі запити оброблено успішно."


# Допоміжні функції для керування кешем
def clear_cache():
    global query_cache, _schema_cache
    query_cache.clear()
    _schema_cache = None
    find_matches_with_ai_cached.cache_clear()


def get_cache_stats():
    return {
        "query_cache_size": len(query_cache),
        "ai_cache_info": find_matches_with_ai_cached.cache_info()
    }