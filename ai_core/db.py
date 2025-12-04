# -*- coding: utf-8 -*-
"""BigQuery helper with simple caching."""

import time
import hashlib
import logging
from typing import Dict, Tuple

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

from .config import BQ_PROJECT

logger = logging.getLogger("ai-bot")

# BQ client
bq_client = bigquery.Client(project=BQ_PROJECT)

# cache: key -> (df, ts)
_query_cache: Dict[str, Tuple[pd.DataFrame, float]] = {}
_CACHE_TTL = 300.0  # seconds
_MAX_ITEMS = 32


def _norm_sql(sql: str) -> str:
    return " ".join(sql.strip().split()).lower()


def _cache_key(sql: str) -> str:
    return hashlib.md5(_norm_sql(sql).encode("utf-8")).hexdigest()


def execute_query(sql: str) -> pd.DataFrame:
    """Execute SQL with a tiny in‑memory cache."""
    now = time.time()
    key = _cache_key(sql)

    if key in _query_cache:
        df, ts = _query_cache[key]
        age = now - ts
        if age < _CACHE_TTL:
            logger.info("[db] cache HIT key=%s age=%.1fs rows=%d", key[:8], age, len(df))
            return df.copy()

    logger.info("[db] cache MISS key=%s", key[:8])
    start = time.perf_counter()
    job = bq_client.query(sql)

    try:
        df = job.result().to_dataframe()
        took = time.perf_counter() - start
        logger.info("[db] OK job_id=%s rows=%d time=%.3fs", job.job_id, len(df), took)
        _query_cache[key] = (df.copy(), now)

        # trim cache
        if len(_query_cache) > _MAX_ITEMS:
            oldest = min(_query_cache, key=lambda k: _query_cache[k][1])
            _query_cache.pop(oldest, None)

        return df
    except BadRequest as e:
        msg = getattr(e, "message", str(e))
        logger.exception("[db] BadRequest job_id=%s : %s", getattr(job, "job_id", "?"), msg)
        raise
    except Exception:
        logger.exception("[db] FAILED job_id=%s", getattr(job, "job_id", "?"))
        raise


def clear_query_cache():
    _query_cache.clear()
