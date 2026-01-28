conftest_code = """
import sys
import os
import pytest
from unittest.mock import MagicMock

# Встановлюємо фейкові змінні оточення
os.environ["BIGQUERY_PROJECT"] = "test-project"
os.environ["BQ_DATASET"] = "test_dataset"
os.environ["BQ_REVENUE_TABLE"] = "rev_tbl"
os.environ["BQ_COST_TABLE"] = "cost_tbl"

# --- ГОЛОВНИЙ ХАК ---
# Ми підміняємо модулі Google ще ДО того, як будь-який тест почне їх імпортувати.
# Це робиться на рівні глобального імпорту.

mock_bq = MagicMock()
mock_vertex = MagicMock()
mock_aiplatform = MagicMock()

# Налаштовуємо мок для BigQuery Client
mock_client = MagicMock()
mock_bq.Client.return_value = mock_client

# Коли код просить get_table, повертаємо фейкову таблицю зі схемою
mock_table = MagicMock()
mock_table.schema = [
    MagicMock(name="date", field_type="DATE"),
    MagicMock(name="revenue", field_type="FLOAT"),
    MagicMock(name="account_no", field_type="INTEGER"),
    MagicMock(name="event_type", field_type="STRING"),
    MagicMock(name="geo_country", field_type="STRING"),
]
mock_client.get_table.return_value = mock_table

# Жорстко записуємо фейки в sys.modules
sys.modules["google.cloud"] = MagicMock()
sys.modules["google.cloud.bigquery"] = mock_bq
sys.modules["google.cloud.aiplatform"] = mock_aiplatform
sys.modules["vertexai"] = mock_vertex
sys.modules["vertexai.preview.generative_models"] = MagicMock()

@pytest.fixture(autouse=True)
def _setup_env():
    # Цей фікстур просто для гарантії, основна робота зроблена вище
    pass
"""

with open("tests/conftest.py", "w") as f:
    f.write(conftest_code)
print("✅ conftest.py оновлено.")
