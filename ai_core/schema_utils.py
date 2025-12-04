# -*- coding: utf-8 -*-
"""Helpers for reading and caching table schemas."""

import time
from typing import Dict, List, Tuple

from google.cloud import bigquery

from .config import BQ_PROJECT

_bq_client = bigquery.Client(project=BQ_PROJECT)

_schema_cache: Dict[str, Tuple[List[dict], float]] = {}
_SCHEMA_TTL = 3600.0  # seconds


def _get_table_schema(table_ref: str):
    now = time.time()
    if table_ref in _schema_cache:
        schema, ts = _schema_cache[table_ref]
        if now - ts < _SCHEMA_TTL:
            return schema
    table = _bq_client.get_table(table_ref)
    schema = [{"name": f.name, "type": f.field_type} for f in table.schema]
    _schema_cache[table_ref] = (schema, now)
    return schema


def get_all_schemas(revenue_ref: str, cost_ref: str):
    rev_schema = _get_table_schema(revenue_ref)
    try:
        cost_schema = _get_table_schema(cost_ref)
    except Exception:
        cost_schema = []
    return rev_schema, cost_schema


def collect_date_columns(schema_list):
    return {
        f["name"]
        for f in schema_list
        if f.get("type") in ("DATE", "DATETIME", "TIMESTAMP")
    }
