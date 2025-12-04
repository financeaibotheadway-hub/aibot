# -*- coding: utf-8 -*-
"""
Local analytics core without any ai_core dependencies.
Fully Cloud Run–safe version.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger("ai-bot")


# ============================
# 1. SIMPLE INTENT CLASSIFIER
# ============================

class Intent:
    def __init__(self, kind: str):
        self.kind = kind


def classify_intent(text: str) -> Intent:
    """Very simple classifier to remove ai_core dependency."""
    t = text.lower()

    if "зменш" in t or "просі" in t or "fall" in t or "drop" in t:
        return Intent("trend_root_cause")

    if "порівня" in t or "compare" in t:
        return Intent("trend_compare")

    if "sql" in t or "запит" in t or "query" in t:
        return Intent("sql_query")

    return Intent("general")


# ============================
# 2. SIMPLE PERIOD PARSER
# ============================

def extract_period(text: str) -> Dict[str, Any]:
    """
    Dummy period parser.
    You can extend it later, but now it prevents Cloud Run crashes.
    """
    return {"period_type": "auto", "raw": text}


# ============================
# 3. TREND ANALYSIS HANDLER
# ============================

def answer_trend_question(message: str, intent_type: str, period_info: dict) -> str:
    """
    Fake trend analysis response.
    The goal: remove ai_core dependency but keep Slack working.
    """
    return (
        f"🔍 Trend analysis placeholder\n"
        f"Intent: {intent_type}\n"
        f"Period: {period_info}\n"
        f"Message: {message}"
    )


# ============================
# 4. GENERIC SQL GENERATOR
# ============================

def run_generic_sql(message: str) -> str:
    """
    Dummy SQL generator. Replace later with real LLM-based generator.
    """
    return (
        "📊 SQL generator placeholder.\n"
        f"Your query intention: {message}\n"
        "Replace this block with real BigQuery SQL generation."
    )


# ============================
# 5. MAIN ENTRY POINT
# ============================

def run_analysis(message: str, semantic_map: dict, user_id: str = "unknown") -> str:
    """
    Main orchestration function used by Slack handler.
    """

    if not message or not message.strip():
        return "Повідомлення порожнє. Напиши інструкцію."

    try:
        intent = classify_intent(message)
        logger.info(f"[analysis] user={user_id} intent={intent.kind} msg={message}")

        # Trend analysis
        if intent.kind in {"trend_root_cause", "trend_compare"}:
            period_info = extract_period(message)
            return answer_trend_question(
                message=message,
                intent_type=intent.kind,
                period_info=period_info
            )

        # SQL generation
        return run_generic_sql(message)

    except Exception as e:
        logger.exception("run_analysis fatal error")
        return "❌ Помилка під час обробки: " + str(e)
