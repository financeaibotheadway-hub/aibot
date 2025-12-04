# -*- coding: utf-8 -*-
"""
Wrapper for Slack handler → Analytics core logic.
"""

import logging
from .analytics_core import run_analysis

logger = logging.getLogger("ai-bot")

def process_slack_message(message: str, semantic_map: dict, user_id: str = "unknown") -> str:
    """
    Entry point для Slack і API.
    """
    try:
        return run_analysis(message, semantic_map, user_id)
    except Exception as e:
        logger.exception("process_slack_message fatal error")
        return "❌ Помилка обробки Slack повідомлення: " + str(e)
