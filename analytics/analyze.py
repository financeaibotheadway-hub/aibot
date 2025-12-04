# analytics/analyze.py
# -*- coding: utf-8 -*-

"""
Обгортка над основним аналітичним модулем.

Slack handler викликає:
    run_analysis(user_text, semantic_map, user_id)

Тут ми пробрасываем цей виклик у process_slack_message()
з модуля analytics_core.
"""

from typing import Dict, Any

from .analytics_core import process_slack_message


def run_analysis(user_text: str, smap: Dict[str, Any], user_id: str = "unknown") -> str:
    """
    Головна точка входу для Slack-бота.

    :param user_text: текст повідомлення користувача з Slack
    :param smap: semantic_map (словник з правилами)
    :param user_id: Slack user id
    :return: готова текстова відповідь для Slack
    """
    try:
        # Використовуємо твій повний аналітичний пайплайн
        return process_slack_message(user_text, smap, user_id=user_id)
    except Exception as e:
        # Щоб у Slack хоч щось прилетіло, навіть якщо аналітика впала
        return f"❌ Помилка у run_analysis: {str(e)}"
