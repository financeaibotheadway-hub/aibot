# aibot/analytics/metric_loader.py
# -*- coding: utf-8 -*-

import logging
from google.cloud import bigquery
from ai_core.config import (
    BQ_PROJECT,
    BQ_DATASET,
    BQ_REVENUE_TABLE,
    BQ_COST_TABLE,
)

logger = logging.getLogger(__name__)
_cache = None


def _schema(table):
    try:
        client = bigquery.Client()
        table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table}"
        t = client.get_table(table_ref)
        return [field.name for field in t.schema]
    except Exception as e:
        logger.error(f"Schema load failed for {table}: {e}")
        return []


def get_metrics() -> list[str]:
    global _cache
    if _cache is not None:
        return _cache

    cols = set()
    cols.update(_schema(BQ_REVENUE_TABLE))
    cols.update(_schema(BQ_COST_TABLE))

    _cache = sorted(cols)
    return _cache
