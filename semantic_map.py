# semantic_map.py
# -*- coding: utf-8 -*-

import os
import time
import json
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Налаштування BigQuery
BQ_PROJECT = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET = os.getenv("BQ_DATASET", "uploads")
BQ_MAP_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.semantic_map_dynamic"

bq_client = bigquery.Client(project=BQ_PROJECT)

# Кеш, щоб не смикати базу постійно (оновлюється раз на 10 хв)
_map_cache = None
_map_cache_time = 0
CACHE_TTL = 600

# =========================================================
# 1. СТАТИЧНА КАРТА (Ваша база)
# =========================================================
STATIC_MAP = {
    "stream:Web": ["web", "веб", "сайт", "w0"],
    "stream:iOS": ["ioc", "ios", "епл", "на еплі", "айос"],
    "stream:Android": ["андроїд", "android", "андройд", "гугл", "gp"],

    "app_name:impulse": ["імпульс", "impulse"],
    "app_name:headway": ["хедвей", "headway"],
    "app_name:skillsta": ["скіллста", "skillsta", "скілста", "skilsta"],
    "app_name:nibble": ["nibble", "nible", "нібл", "ніббл"],
    "app_name:addmile": ["addmile", "admile", "едмайл", "адмайл"],

    "revenue_type:New": ["new", "нью", "нового"],
    "revenue_type:Retained": ["retained", "ретейнд", "старий", "старого", "ретеінд"],

    "event_type:commission": ["комісії", "commission", "комісія"],
    "event_type:sale": [
        {"text": "продажі", "w": 1.0}, {"text": "сейл", "w": 1.0}, 
        {"text": "сейлзи", "w": 1.0}, {"text": "sale", "w": 1.0}, 
        {"text": "ревенью", "w": 1.0}
    ],
    "event_type:trial": ["тріал", "тряал", "trial", "трайал"],
    "event_type:refund": [
        {"text": "рефанд", "w": 1.0}, {"text": "refund", "w": 1.0}, "повернення"
    ],
    "event_type:chargeback": [
        {"text": "чарджбек", "w": 1.0}, {"text": "chargeback", "w": 1.0}, "скасування"
    ],
    "event_type:refund_fee": [
        {"text": "комісія рефанд", "w": 1.0}, {"text": "refund fee", "w": 1.0}, 
        "штраф за повернення", {"text": "рефанд фі", "w": 1.0}
    ],
    "event_type:chargeback_fee": ["комісія чарджбек", "chargeback fee", "штраф за скасування", "фі чарджбек"],
    "event_type:vat": ["vat", "пдв", "ПДВ", "Податок додану вартість"],
    "event_type:wht": ["wht", "вхт", "Пнр", "Податок на репатріацію"],
    "event_type:opex": ["opex", "опекс", "опекси", "Операційні витрати"],

    "processing_legal_entity:GTHW": ["GTHW", "ГТХВ"],
    "processing_legal_entity:Milibro": ["Milibro", "Мілібро"],
    "processing_legal_entity:Kremital": ["Kremital", "Кремітал"],
    "processing_legal_entity:Vodelif": ["Vodelif", "Воделіф"],
    "processing_legal_entity:Librotech": ["Librotech", "Лібротех"]
}

# =========================================================
# 2. ДИНАМІЧНА ЛОГІКА
# =========================================================

def _ensure_table_exists():
    try:
        bq_client.get_table(BQ_MAP_TABLE)
    except NotFound:
        # Таблиця має бути створена SQL скриптом, але про всяк випадок:
        pass

def get_semantic_map(force_refresh=False):
    """Повертає об'єднану карту: STATIC + BIGQUERY"""
    global _map_cache, _map_cache_time
    
    if not force_refresh and _map_cache and (time.time() - _map_cache_time < CACHE_TTL):
        return _map_cache

    combined_map = {k: v[:] if isinstance(v, list) else v for k, v in STATIC_MAP.items()}
    
    try:
        query = f"SELECT term_key, term_value FROM `{BQ_MAP_TABLE}`"
        rows = list(bq_client.query(query).result())
        
        for row in rows:
            key = row.term_key
            val = row.term_value
            if key in combined_map:
                # Перевірка на дублі
                current_vals = combined_map[key]
                exists = False
                for item in current_vals:
                    if isinstance(item, str) and item.lower() == val.lower(): exists = True
                    elif isinstance(item, dict) and item.get("text", "").lower() == val.lower(): exists = True
                
                if not exists:
                    combined_map[key].append(val)
            else:
                combined_map[key] = [val]
                
        _map_cache = combined_map
        _map_cache_time = time.time()
    except Exception as e:
        print(f"⚠️ Error loading dynamic map: {e}")
        return combined_map

    return combined_map

def add_term_to_map(key, value):
    """Додає нове слово в BigQuery"""
    try:
        # Перевірка дублів в базі
        check_sql = f"SELECT count(1) as cnt FROM `{BQ_MAP_TABLE}` WHERE term_key = @key AND term_value = @val"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("key", "STRING", key),
                bigquery.ScalarQueryParameter("val", "STRING", value)
            ]
        )
        res = list(bq_client.query(check_sql, job_config=job_config).result())
        if res and res[0].cnt > 0:
            return False

        rows = [{"term_key": key, "term_value": value, "created_at": time.strftime('%Y-%m-%d %H:%M:%S')}]
        errors = bq_client.insert_rows_json(BQ_MAP_TABLE, rows)
        
        if not errors:
            print(f"✅ Learned term: {key} -> {value}")
            global _map_cache_time
            _map_cache_time = 0 # Скидаємо кеш
            return True
        else:
            print(f"❌ BQ Error: {errors}")
            return False
    except Exception as e:
        print(f"❌ Exception adding term: {e}")
        return False

# Змінна для сумісності
semantic_map = get_semantic_map()
