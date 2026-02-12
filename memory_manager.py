import json
import os
import uuid
import difflib
from datetime import datetime

MEMORY_FILE = "bot_memory.json"
LEARNED_MAP_FILE = "semantic_map_learned.json"

def load_data(filename):
    if not os.path.exists(filename):
        return [] if filename == MEMORY_FILE else {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return [] if filename == MEMORY_FILE else {}

def save_data(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

# --- MEMORY LOGIC ---

def log_query(user_query, sql, ai_response):
    """Зберігає запит як 'pending' (без оцінки) і повертає ID"""
    data = load_data(MEMORY_FILE)
    query_id = str(uuid.uuid4())[:8]  # Короткий ID
    
    record = {
        "id": query_id,
        "timestamp": str(datetime.now()),
        "query": user_query,
        "sql": sql,
        "response": ai_response,
        "rating": None  # good, bad_sql, bad_context
    }
    data.append(record)
    # Тримаємо останні 1000 записів
    if len(data) > 1000:
        data = data[-1000:]
    
    save_data(MEMORY_FILE, data)
    return query_id

def update_rating(query_id, rating):
    """Оновлює оцінку запиту"""
    data = load_data(MEMORY_FILE)
    record = None
    for item in data:
        if item["id"] == query_id:
            item["rating"] = rating
            record = item
            break
    save_data(MEMORY_FILE, data)
    return record

def get_similar_examples(user_query):
    """Шукає схожі запити з оцінкою 'good'"""
    data = load_data(MEMORY_FILE)
    good_examples = [d for d in data if d.get("rating") == "good"]
    
    found = []
    # Простий пошук схожості тексту (можна замінити на Embeddings пізніше)
    for ex in good_examples:
        similarity = difflib.SequenceMatcher(None, user_query.lower(), ex["query"].lower()).ratio()
        if similarity > 0.4: # 40% схожості
            found.append(f"Q: {ex['query']}\nSQL: {ex['sql']}")
    
    # Повертаємо топ-3 найновіших схожих
    return "\n---\n".join(found[-3:])

# --- SEMANTIC MAP LOGIC ---

def load_learned_map():
    return load_data(LEARNED_MAP_FILE)

def update_learned_map(new_terms):
    current = load_learned_map()
    current.update(new_terms)
    save_data(LEARNED_MAP_FILE, current)
    return current
