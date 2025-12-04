# -*- coding: utf-8 -*-
"""
Unified entry point for Slack handler.
Implements 4-model pipeline:
1. intent detection
2. period parsing (if needed)
3. trend analysis (root cause / compare)
4. generic SQL generation (via AI)
"""

import logging

from ai_core.intents import classify_intent
from ai_core.period_parser import extract_period
from ai_core.trend_analysis import answer_trend_question
from ai_core.generic_sql import run_generic_sql

logger = logging.getLogger("ai-bot")


def run_analysis(message: str, semantic_map: dict, user_id: str = "unknown") -> str:
    """
    Main orchestrator for analytics logic.
    Called from slack_handler.py

    message → classify_intent → route to handler
    """

    if not message or not message.strip():
        return "Повідомлення порожнє. Напиши інструкцію."

    try:
        # 1) Визначаємо інтенцію
        intent = classify_intent(message)
        logger.info(f"[analysis] user={user_id} intent={intent.kind} msg={message}")

        # 2) Якщо питання про тренди (падіння, причини, зростання)
        if intent.kind in {"trend_root_cause", "trend_compare"}:

            period_info = extract_period(message)     # "травень 2024", "за останні 30 днів" etc.
            return answer_trend_question(
                message=message,
                intent_type=intent.kind,
                period_info=period_info
            )

        # 3) Всі інші запити → AI SQL генерація
        return run_generic_sql(message)

    except Exception as e:
        logger.exception("run_analysis fatal error")
        return "❌ Помилка під час обробки: " + str(e)
