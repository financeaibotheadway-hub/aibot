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
    """Зберігає запит як 'pending'"""
    query_id = str(uuid.uuid4())[:8]
    row = {
        "id": query_id,
        "timestamp": datetime.now().isoformat(),
        "query": user_query.strip(),
        "sql": sql,
        "response_text": response_text[:50000],
        "rating": None
    }
    try:
        bq_client.insert_rows_json(BQ_MEMORY_TABLE, [row])
    except Exception as e:
        print(f"Memory Log Error: {e}")
    return query_id

def update_rating(query_id, rating):
    """
    Оновлює оцінку. Якщо Good -> запускає навчання.
    """
    update_sql = f"UPDATE `{BQ_MEMORY_TABLE}` SET rating = @rating WHERE id = @id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("rating", "STRING", rating),
            bigquery.ScalarQueryParameter("id", "STRING", query_id)
        ]
    )
    try:
        bq_client.query(update_sql, job_config=job_config).result()
        print(f"⭐️ Rated {query_id}: {rating}")
        
        if rating == "good":
            # Тягнемо дані для навчання
            sel_sql = f"SELECT query, sql FROM `{BQ_MEMORY_TABLE}` WHERE id = @id LIMIT 1"
            rows = list(bq_client.query(sel_sql, job_config=job_config).result())
            if rows:
                _learn_semantics(rows[0].query, rows[0].sql)
    except Exception as e:
        print(f"Rating Update Error: {e}")

def find_exact_match(user_query):
    """Перевіряє, чи є такий успішний запит в базі"""
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
    """Шукає схожі запити (Top-3)"""
    try:
        # Беремо останні 500 успішних і фільтруємо в Python (швидше і дешевше)
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
    """AI Агент: шукає нові слова і пише їх в мапу"""
    print(f"🎓 Learning from: {user_query}")
    current_map = get_semantic_map()
    model = GenerativeModel("gemini-2.5-flash")
    
    prompt = f"""
    You are a Semantic Learner.
    Query: "{user_query}"
    SQL: "{sql}"
    Current Knowledge: {json.dumps(current_map, ensure_ascii=False)[:3000]}...
    
    Task: Identify specific synonyms/terms in Query that map to SQL columns/values but are MISSING in Knowledge.
    Return JSON format: {{ "existing_key": "new_term" }}
    Example: {{ "stream:Web": "інтернет" }}
    If empty, return {{}}
    """
    try:
        resp = model.generate_content(prompt)
        txt = resp.text.strip().replace("```json", "").replace("```", "")
        new_terms = json.loads(txt)
        if new_terms:
            for k, v in new_terms.items():
                add_term_to_map(k, v)
    except Exception as e:
        print(f"Learning Error: {e}")
