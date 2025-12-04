# -*- coding: utf-8 -*-
"""Central config for Finance AI bot."""

import os

BQ_PROJECT       = os.getenv("BIGQUERY_PROJECT", "finance-ai-bot-headway")
BQ_DATASET       = os.getenv("BQ_DATASET", "uploads")
BQ_REVENUE_TABLE = os.getenv("BQ_REVENUE_TABLE", "revenue_test_databot")
BQ_COST_TABLE    = os.getenv("BQ_COST_TABLE", "cost_test_databot")
VERTEX_LOCATION  = os.getenv("VERTEX_LOCATION", "europe-west1")
LOCAL_TZ         = os.getenv("LOCAL_TZ", "Europe/Kyiv")

LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO").upper()

RETURN_SQL_ON_ERROR = os.getenv("RETURN_SQL_ON_ERROR", "false").lower() == "true"
AI_BOT_WORKERS   = int(os.getenv("AI_BOT_WORKERS", "4"))
