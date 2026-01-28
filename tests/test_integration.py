# tests/test_integration.py
import pytest
import os
import importlib
from analytics import analytics_core

# Маркуємо тест як "integration", щоб можна було запускати окремо
@pytest.mark.integration
def test_generate_sql_real_ai():
    """
    Цей тест надсилає реальний запит до Gemini і перевіряє, 
    чи валідний SQL він повертає.
    Потрібен файл ключа .json!
    """
    
    # Перевіряємо, чи є ключі. Якщо ні - пропускаємо тест.
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        pytest.skip("No Google Credentials found, skipping AI integration test")

    # Приклад мапи (можна взяти реальну з semantic_map.py)
    smap = {
        "revenue_table": ["revenue", "gross_revenue", "date", "country"],
        "cost_table": ["amount", "cost_center", "account_no"]
    }

    # Запит користувача
    user_prompt = "Який дохід був учора в США?"

    try:
        # Викликаємо реальну функцію генерації
        sql = analytics_core.generate_sql(user_prompt, smap)
        
        print(f"\nGenerated SQL: {sql}")

        # ПЕРЕВІРКИ (Assertions)
        assert "SELECT" in sql.upper()
        assert "WHERE" in sql.upper()
        # Перевіряємо, чи AI зрозумів про США
        assert "US" in sql or "USA" in sql or "United States" in sql
        # Перевіряємо, чи взяв правильну таблицю
        assert "revenue" in sql.lower()

    except Exception as e:
        pytest.fail(f"AI generation failed: {e}")
