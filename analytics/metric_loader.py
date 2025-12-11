# analytics/metric_loader.py
# -*- coding: utf-8 -*-

import logging
import os
from google.cloud import bigquery

logger = logging.getLogger(__name__)
_cache = None

# ───────────────────────────────────────────────
# Load from ENV (як в analytics_core.py)
# ───────────────────────────────────────────────
BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")


def _schema(table):
    """
    Читаємо схему таблиці з BigQuery, повертаємо список колонок.
    """
    try:
        client = bigquery.Client(project=BQ_PROJECT)
        table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
        t = client.get_table(table_ref)
        return [field.name for field in t.schema]
    except Exception as e:
        logger.error(f"Schema load failed for {table}: {e}")
        return []


def get_metrics() -> list[str]:
    """
    Повертаємо унікальний список всіх колонок revenue та cost таблиць.
    Кешуємо результат.
    """
    global _cache
    if _cache is not None:
        return _cache

    cols = set()
    cols.update(_schema(BQ_REVENUE_TABLE))
    cols.update(_schema(BQ_COST_TABLE))

    _cache = sorted(cols)
    return _cache
