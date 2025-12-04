# -*- coding: utf-8 -*-
import re

class Intent:
    def __init__(self, kind: str):
        self.kind = kind

def classify_intent(message: str) -> Intent:
    """
    Дуже простий класифікатор інтенцій.
    Якщо треба — замінимо на AI-класифікацію.
    """
    msg = message.lower()

    if "чому" in msg or "поясни" in msg:
        return Intent("trend_root_cause")
    
    if "порівняй" in msg or "compare" in msg:
        return Intent("trend_compare")

    # default — generic SQL
    return Intent("generic_sql")
