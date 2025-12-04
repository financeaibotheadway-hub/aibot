# -*- coding: utf-8 -*-

def extract_period(message: str):
    """
    Витягує період типу:
    - "2021"
    - "2020-2021"
    - "за останні 6 місяців"
    - "у листопаді 2023"
    - "по місяцях у 2021 році"
    """

    msg = message.lower()

    # формати конкретних років
    import re
    year_range = re.findall(r"(20\d{2})\s*[-–]\s*(20\d{2})", msg)
    if year_range:
        start, end = year_range[0]
        return {"type": "year_range", "start": int(start), "end": int(end)}

    year = re.findall(r"20\d{2}", msg)
    if year:
        return {"type": "year", "year": int(year[0])}

    # місяці
    months = {
        "січ": 1, "лют": 2, "бер": 3, "кві": 4, "тра": 5, "чер": 6,
        "лип": 7, "сер": 8, "вер": 9, "жов": 10, "лис": 11, "гру": 12
    }

    for m in months:
        if m in msg:
            return {"type": "month", "month": months[m]}

    return {"type": "unknown"}
