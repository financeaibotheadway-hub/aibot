# aibot/analytics/metric_parser.py
# -*- coding: utf-8 -*-

import re
from .metric_loader import get_metrics


def detect_metric(msg: str) -> str | None:
    message = msg.lower()
    metrics = get_metrics()

    synonyms = {}

    for m in metrics:
        base = m.lower()
        syn = {
            base,
            base.replace("_", " "),
        }

        # додаткові словники
        if "rev" in base or "revenue" in base:
            syn.update({"rev", "revenue", "дохід", "виручка", "sales"})
        if "cost" in base or "opex" in base or "expense" in base:
            syn.update({"opex", "затрати", "витрати", "cost", "спенд"})

        synonyms[m] = syn

    for col, tokens in synonyms.items():
        for t in tokens:
            if t and t in message:
                return col

    return None
