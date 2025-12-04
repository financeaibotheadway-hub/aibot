# -*- coding: utf-8 -*-
"""Top‑level API used by Slack handler.

Exports:
- process_slack_message(message, semantic_map, user_id)
- process_slack_message_async(...)
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from .intents import classify_intent
from .trend_analysis import answer_trend_question
from .generic_sql import run_generic_sql

logger = logging.getLogger("ai-bot")

_executor = ThreadPoolExecutor(max_workers=4)


def _run_in_executor(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(_executor, lambda: func(*args, **kwargs))


def _handle_single(message: str, semantic_map: dict, user_id: str = "unknown") -> str:
    intent = classify_intent(message)
    logger.info("[analytics] user_id=%s intent=%s message=%s", user_id, intent.kind, message)

    if intent.kind in {"trend_root_cause", "trend_compare"}:
        return answer_trend_question(message, intent.kind)

    # For now, all other queries go via generic SQL path (AI builds SQL)
    return run_generic_sql(message)


def process_slack_message(message: str, smap: dict, user_id: str = "unknown") -> str:
    msg = message or ""
    if not msg.strip():
        return "Повідомлення порожнє. Напиши інструкцію."
    try:
        return _handle_single(msg, smap, user_id)
    except Exception as e:
        logger.exception("[process_slack_message] fatal")
        return "Помилка під час обробки повідомлення: " + (getattr(e, "message", None) or str(e))


async def process_slack_message_async(message: str, smap: dict, user_id: str = "unknown") -> str:
    msg = message or ""
    if not msg.strip():
        return "Повідомлення порожнє. Напиши інструкцію."
    return await _run_in_executor(_handle_single, msg, smap, user_id)
