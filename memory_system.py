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
    """
    ### ВИПРАВЛЕННЯ ###
    Зберігає запит як 'pending' за допомогою DML INSERT, а не стрімінгу.
    Це вирішує проблему "streaming buffer".
    """
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
        query_job.result() # Чекаємо завершення
        if query_job.errors:
            print(f"Memory Log DML Error: {query_job.errors}")
    except Exception as e:
        print(f"Memory Log DML Exception: {e}")
        
    return query_id

def update_rating(query_id, rating):
    """
    Оновлює оцінку за допомогою MERGE. Тепер це буде працювати,
    оскільки початковий запис теж був DML.
    """
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

# --- (решта файлу без змін) ---

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
