import logging
from vertexai.generative_models import GenerativeModel, Part

logger = logging.getLogger("ai-bot")

model = GenerativeModel("gemini-1.5-flash")

def run_generic_sql(message: str) -> str:
    """
    Generate SQL using Gemini.
    """
    try:
        prompt = f"Generate BigQuery SQL for the following user request:\n{message}"
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        logger.exception("generic_sql fatal error")
        return "❌ Помилка генерації SQL: " + str(e)
