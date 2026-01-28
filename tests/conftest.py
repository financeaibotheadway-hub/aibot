# tests/conftest.py
import sys
import os
import pytest
from unittest.mock import MagicMock

# 1. Додаємо корінь проекту в шлях імпорту, щоб бачити модуль analytics
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 2. Встановлюємо тестові змінні оточення
os.environ["BIGQUERY_PROJECT"] = "test-project"
os.environ["BQ_DATASET"] = "test_dataset"
os.environ["BQ_REVENUE_TABLE"] = "revenue_table"
os.environ["BQ_COST_TABLE"] = "cost_table"

# 3. Мокаємо (імітуємо) BigQuery та VertexAI, щоб при імпорті analytics_core не було помилки авторизації
# Це потрібно, якщо ми хочемо тестувати тільки логіку (test_logic.py) без ключів
@pytest.fixture(autouse=True)
def mock_google_clients(monkeypatch):
    mock_bq = MagicMock()
    mock_vertex = MagicMock()
    
    # Підміняємо реальні бібліотеки на фейкові для тестів
    monkeypatch.setitem(sys.modules, 'google.cloud.bigquery', mock_bq)
    monkeypatch.setitem(sys.modules, 'vertexai', mock_vertex)
    monkeypatch.setitem(sys.modules, 'vertexai.preview.generative_models', MagicMock())
