import json
import vertexai
from vertexai.preview.generative_models import GenerativeModel
from memory_manager import update_learned_map, load_learned_map

# Імпортуємо статичну карту, щоб знати, що ми вже знаємо
try:
    from semantic_map import semantic_map as static_map
except ImportError:
    static_map = {}

def learn_new_semantics(user_query, sql):
    """
    Аналізує вдалий запит і витягує нові терміни.
    """
    learned_map = load_learned_map()
    
    # Об'єднуємо знання
    full_knowledge = {**static_map, **learned_map}
    
    model = GenerativeModel("gemini-2.5-flash")
    
    prompt = f"""
    You are a SQL AI Trainer.
    User Query: "{user_query}"
    Generated SQL: "{sql}"
    
    Current Semantic Map (Known terms):
    {json.dumps(full_knowledge, ensure_ascii=False)[:5000]}... (truncated)
    
    TASK:
    Analyze the User Query. Did the user use a specific synonym, slang, or business term that maps to a SQL table/column/value, which is NOT explicitly in the Semantic Map?
    
    If YES, return a JSON object with the new mapping.
    Format: {{"new_term": "sql_column_name" or "value"}}
    
    If NO new terms found, return empty JSON {{}}.
    
    Return ONLY JSON. No markdown.
    """
    
    try:
        response = model.generate_content(prompt, generation_config={"temperature": 0.1})
        text = response.text.strip().replace("```json", "").replace("```", "")
        new_terms = json.loads(text)
        
        if new_terms:
            print(f"🧠 Learned new terms: {new_terms}")
            update_learned_map(new_terms)
            return new_terms
    except Exception as e:
        print(f"Learning error: {e}")
        return {}
