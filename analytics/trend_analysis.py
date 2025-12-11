# aibot/analytics/trend_analysis.py
# -*- coding: utf-8 -*-

from .metric_parser import detect_metric
from .period_parser import extract_period
from .metric_loader import get_metrics
from google.cloud import bigquery
from ai_core.config import (
    BQ_PROJECT,
    BQ_DATASET,
    BQ_REVENUE_TABLE,
    BQ_COST_TABLE,
)


def answer_trend_question(message: str, kind: str) -> str:
    """
    Головна функція, яку викликає analytics_core.
    """

    if kind == "trend_root_cause":
        return "Root-cause аналіз скоро буде доступний."

    if kind == "trend_compare":
        return run_trend_compare(message)

    return run_trend_analysis(message)


def run_trend_analysis(message: str) -> str:
    """
    Реальний трендовий аналіз: визначає метрику, період, будує SQL.
    """

    metric = detect_metric(message)

    if not metric:
        available = ", ".join(get_metrics()[:20])
        return (
            "Я не зміг визначити метрику у запиті.\n"
            "Скажи, будь ласка, що аналізувати — opex, revenue, cost, sales?\n\n"
            f"Доступні поля: {available} ..."
        )

    period = extract_period(message)

    sql = f"""
        SELECT
            date,
            {metric} AS metric_value
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_REVENUE_TABLE}`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {period} DAY)
        ORDER BY date
    """

    client = bigquery.Client()
    df = client.query(sql).to_dataframe()

    if df.empty:
        return f"Даних по '{metric}' за період не знайдено."

    latest = df.metric_value.iloc[-1]
    trend = df.metric_value.pct_change().iloc[-1] * 100

    return (
        f"Метрика **{metric}** за останні {period} днів:\n\n"
        f"• Поточне значення: {latest:,.2f}\n"
        f"• Зміна до попереднього дня: {trend:,.1f}%"
    )


def run_trend_compare(message: str) -> str:
    return "Функція порівняння періодів ще в розробці."
