# analytics/trend_analysis.py
# -*- coding: utf-8 -*-

import logging
import os
import pandas as pd
from google.cloud import bigquery

# Нові імпорти — ТІЛЬКИ наші
from analytics.metric_loader import get_metrics
from analytics.metric_parser import detect_metric

logger = logging.getLogger(__name__)


# ───────────────────────────────────────────────
# ENV
# ───────────────────────────────────────────────
BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")

bq_client = bigquery.Client(project=BQ_PROJECT)


# ───────────────────────────────────────────────
# SIMPLE TREND CALCULATOR
# ───────────────────────────────────────────────
def run_trend_analysis(df: pd.DataFrame, metric: str) -> str:
    """
    Проста аналітика трендів:
    - визначаємо зростання/падіння
    - даємо коротке пояснення

    df — результат виконання SQL
    metric — якої метрики стосується запит
    """

    if df.empty or metric not in df.columns:
        return f"Не можу виконати тренд-аналіз для метрики: {metric}"

    try:
        values = df[metric].dropna().astype(float)
        if len(values) < 2:
            return "Недостатньо даних для аналізу тренду."

        diff = values.iloc[-1] - values.iloc[-2]
        pct = (diff / values.iloc[-2]) * 100 if values.iloc[-2] != 0 else 0

        if diff > 0:
            trend = "📈 Зростання"
        elif diff < 0:
            trend = "📉 Падіння"
        else:
            trend = "➖ Без змін"

        return (
            f"{trend} метрики **{metric}**: "
            f"{diff:.2f} ({pct:.1f}%)\n"
            f"Останні значення: {list(values.tail(5))}"
        )
    except Exception as e:
        logger.error("Trend analysis failed: %s", e)
        return "Помилка тренд-аналізу."
