# -*- coding: utf-8 -*-
"""
Unified entry point for Slack handler.
Implements 4-model pipeline:
1. intent detection
2. period parsing
3. trend analysis (root cause / compare)
4. generic SQL generation
"""

import logging

from .intents import classify_intent
from .period_parser import extract_period
from .trend_analysis import answer_trend_question
from .generic_sql import run_generic_sql

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

        # 2) Якщо питання про тренди
        if intent.kind in {"trend_root_cause", "trend_compare"}:

            period_info = extract_period(message)
            return answer_trend_question(
                message=message,
                intent_type=intent.kind,
                period_info=period_info
            )

        # 3) Інші запити → AI SQL генерація
        return run_generic_sql(message)

    except Exception as e:
        logger.exception("run_analysis fatal error")
        return "❌ Помилка під час обробки: " + str(e)
