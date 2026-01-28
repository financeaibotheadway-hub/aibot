# tests/test_logic.py
import pytest
# Оскільки ми замокали google в conftest, цей імпорт пройде безпечно
from analytics import analytics_core 

# --- Тест 1: Чи правильно витягуємо номери рахунків ---
@pytest.mark.parametrize("text, expected", [
    ("Покажи рахунок 1234", 1234),
    ("Account № 9999 details", 9999),
    ("Просто текст без номера", None),
    ("acct 0000", 0),
])
def test_extract_account_no(text, expected):
    assert analytics_core.extract_account_no(text) == expected

# --- Тест 2: Чи правильно визначаємо рік ---
def test_extract_year():
    assert analytics_core.extract_year("Дані за 2024 рік") == 2024
    assert analytics_core.extract_year("сума 5000 грн") is None  # 5000 - це не рік

# --- Тест 3: Чи правильно визначаємо тип події ---
@pytest.mark.parametrize("text, expected_type", [
    ("чому був refund?", "refund"),
    ("скільки нових підписок", "sale"),
    ("який податок vat", "vat"),
    ("витрати на офіс", None), 
])
def test_detect_event_type(text, expected_type):
    assert analytics_core.detect_event_type(text) == expected_type

# --- Тест 4: Санітизація SQL (щоб не було ділення на нуль) ---
def test_sql_sanitization():
    raw_sql = "SELECT revenue / users FROM table"
    clean_sql = analytics_core._sanitize_division_by_zero(raw_sql)
    assert "SAFE_DIVIDE(revenue, users)" in clean_sql
