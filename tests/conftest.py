import sys
from unittest.mock import MagicMock
import os

# Створюємо код для файлу конфігурації
conftest_content = """
import sys
import os
import pytest
from unittest.mock import MagicMock

# --- ХАК: Глобальні моки (Mocking at Import Time) ---
# Це найважливіша частина. Ми підміняємо бібліотеки ДО того, як вони імпортуються.

# 1. Створюємо фейки
mock_bq = MagicMock()
mock_vertex = MagicMock()
mock_aiplatform = MagicMock()

# 2. Налаштовуємо BigQuery Client, щоб він не падав при виклику get_table
# Це "обманює" рядок: _ = get_all_schemas()
mock_client_instance = MagicMock()
mock_bq.Client.return_value = mock_client_instance

# Створюємо фейкову таблицю зі схемою
mock_table = MagicMock()
mock_table.schema = [
    MagicMock(name="date", field_type="DATE"),
    MagicMock(name="revenue", field_type="FLOAT"),
    MagicMock(name="account_no", field_type="INTEGER"),
]
mock_client_instance.get_table.return_value = mock_table

# 3. Записуємо фейки в системні модулі Python
# Коли analytics_core зробить "from google.cloud import bigquery", він отримає наш mock_bq
sys.modules["google.cloud"] = MagicMock()
sys.modules["google.cloud.bigquery"] = mock_bq
sys.modules["google.cloud.aiplatform"] = mock_aiplatform
sys.modules["vertexai"] = mock_vertex
sys.modules["vertexai.preview.generative_models"] = MagicMock()

# 4. Фейкові змінні оточення (щоб не було помилок os.getenv)
os.environ["BIGQUERY_PROJECT"] = "test-project"
os.environ["BQ_DATASET"] = "test_dataset"
os.environ["BQ_REVENUE_TABLE"] = "rev_tbl"
os.environ["BQ_COST_TABLE"] = "cost_tbl"

# Додаємо шлях до кореня проекту
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

@pytest.fixture(autouse=True)
def _setup_env():
    # Цей фікстур потрібен просто для ініціалізації файлу
    pass
"""

# Записуємо файл
with open("tests/conftest.py", "w") as f:
    f.write(conftest_content)

print("✅ tests/conftest.py оновлено. Тепер чистовик не буде падати.")
