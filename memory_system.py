# memory_system.py
# -*- coding: utf-8 -*-

import os
import uuid
import difflib
import json
import time
from datetime import datetime
from google.cloud import bigquery
import vertexai
from vertexai.preview.generative_models import GenerativeModel

from semantic_map import add_term_to_map, get_semantic_map

BQ_PROJECT = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET = os.getenv("BQ_DATASET", "uploads")
BQ_MEMORY_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.bot_memory"

bq_client = bigquery.Client(project=BQ_PROJECT)

def log_query_to_memory(user_query, sql, response_text):
    query_id = str(uuid.uuid4())[:8]
    insert_sql = f"""
        INSERT INTO `{BQ_MEMORY_TABLE}` (id, timestamp, query, sql, response_text, rating)
        VALUES (@id, @ts, @query, @sql, @response, NULL)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", query_id),
            bigquery.ScalarQueryParameter("ts", "TIMESTAMP", datetime.now().isoformat()),
            bigquery.ScalarQueryParameter("query", "STRING", user_query.strip()),
            bigquery.ScalarQueryParameter("sql", "STRING", sql),
            bigquery.ScalarQueryParameter("response", "STRING", response_text[:50000]),
        ]
    )
    try:
        query_job = bq_client.query(insert_sql, job_config=job_config)
        query_job.result()
        if query_job.errors:
            print(f"Memory Log DML Error: {query_job.errors}")
    except Exception as e:
        print(f"Memory Log DML Exception: {e}")
    return query_id

def update_rating(query_id, rating):
    print(f"Attempting to MERGE rating for {query_id} to '{rating}'...")
    merge_sql = f"""
        MERGE `{BQ_MEMORY_TABLE}` T
        USING (SELECT @id AS id) S ON T.id = S.id
        WHEN MATCHED THEN
          UPDATE SET rating = @rating
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("rating", "STRING", rating),
            bigquery.ScalarQueryParameter("id", "STRING", query_id)
        ]
    )
    try:
        query_job = bq_client.query(merge_sql, job_config=job_config)
        query_job.result()
        if query_job.errors:
            print(f"❌ BQ MERGE Job Error for {query_id}: {query_job.errors}")
            return
        print(f"⭐️ Rated {query_id}: {rating} successfully.")
        if rating == "good":
            sel_sql = f"SELECT query, sql FROM `{BQ_MEMORY_TABLE}` WHERE id = @id LIMIT 1"
            rows = list(bq_client.query(sel_sql, job_config=job_config).result())
            if rows:
                _learn_semantics(rows[0].query, rows[0].sql)
    except Exception as e:
        print(f"FATAL Rating Update/Merge Error for {query_id}: {e}")

def find_exact_match(user_query):
    sql = f"""
        SELECT sql FROM `{BQ_MEMORY_TABLE}`
        WHERE rating = 'good' AND LOWER(TRIM(query)) = LOWER(TRIM(@q))
        ORDER BY timestamp DESC LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("q", "STRING", user_query)]
    )
    try:
        rows = list(bq_client.query(sql, job_config=job_config).result())
        if rows: return rows[0].sql
    except: pass
    return None

def find_similar_matches(user_query):
    try:
        sql = f"SELECT query, sql FROM `{BQ_MEMORY_TABLE}` WHERE rating = 'good' ORDER BY timestamp DESC LIMIT 500"
        rows = list(bq_client.query(sql).result())
        found = []
        for r in rows:
            ratio = difflib.SequenceMatcher(None, user_query.lower(), r.query.lower()).ratio()
            if ratio > 0.55:
                found.append(f"User: {r.query}\nSQL: {r.sql}")
        return "\n---\n".join(list(set(found))[:3])
    except: return ""

def _learn_semantics(user_query, sql):
    """
    ### ОНОВЛЕНИЙ "МОЗОК" ###
    AI Агент, який аналізує запит і шукає нові слова з розумним промптом.
    """
    print(f"🎓 Learning from query: '{user_query}'")
    
    # Завантажуємо актуальну карту знань (статика + динаміка з BQ)
    current_map = get_semantic_map()
    
    model = GenerativeModel("gemini-2.5-flash")
    
    # Новий, більш детальний промпт
    prompt = f"""
    You are an AI Analyst tasked with expanding a semantic map.
    Your goal is to find new synonyms or terms from a user's query that correspond to a value in a generated SQL query, but are not yet in the knowledge base.

    **1. Knowledge Base (Current Semantic Map):**
    ```json
    {json.dumps(current_map, ensure_ascii=False, indent=2)}
    ```

    **2. Input Data:**
    - **User Query:** "{user_query}"
    - **Generated SQL:** "{sql}"

    **3. Your Task (Step-by-step):**
    a. **Analyze the SQL:** Identify key filters in the `WHERE` clause. For example, `app_name = 'nibble'` or `geo_country = 'US'`.
    b. **Analyze the User Query:** Find words or phrases the user said that correspond to these SQL filters. For example, the user might say "небула" which corresponds to `app_name = 'nibble'`.
    c. **Check the Knowledge Base:** For the mapping you found (e.g., "небула" -> 'nibble'), find the correct key in the map (e.g., `app_name:nibble`). Look at the list of synonyms for that key. Is the user's word ("небула") already in that list?
    d. **Identify New Terms:** If the user's word is NOT in the list, it's a new term to be learned.
    e. **Format the Output:** Return a JSON object where the key is the semantic map key and the value is the new synonym.

    **Example:**
    - User Query: "доходи по небулі за 2024"
    - Generated SQL: "SELECT ... FROM ... WHERE app_name = 'nibble' AND ..."
    - Your thinking:
        1. SQL filters by `app_name = 'nibble'`.
        2. User said "небулі". This seems to correspond to 'nibble'.
        3. The map key for this is `app_name:nibble`.
        4. I'll check the list for `app_name:nibble` in the knowledge base. Let's say it is `["nibble", "nible", "нібл", "ніббл"]`.
        5. The word "небулі" is NOT in this list. It is a new term.
    - **Output:**
    ```json
    {{
        "app_name:nibble": "небулі"
    }}
    ```

    **CRITICAL RULES:**
    - If you find no new terms, return an empty JSON object: `{{}}`.
    - Return ONLY the JSON object, nothing else. No markdown, no explanations.
    """
    
    try:
        resp = model.generate_content(prompt, generation_config={"temperature": 0.0})
        text = resp.text.strip().replace("```json", "").replace("```", "")
        
        # Перевірка на пусту відповідь
        if not text or text == "{}":
            print("🧠 No new terms found to learn.")
            return

        new_terms = json.loads(text)
        if new_terms:
            for key, value in new_terms.items():
                # Переконуємося, що ми додаємо просту строку
                if isinstance(value, str):
                    add_term_to_map(key, value)
    except Exception as e:
        print(f"❌ Learning agent failed: {e}. Response text was: '{text}'")
