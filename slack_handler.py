# slack_handler.py
# -*- coding: utf-8 -*-

import os
import json
import logging
import asyncio

from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import JSONResponse

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.signature import SignatureVerifier

from cachetools import TTLCache

from analytics import process_slack_message
from semantic_map import semantic_map

# ──────────────────────────────────────────────────────────────────────────────
# ENV / LOG
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("slack")

SLACK_BOT_TOKEN    = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")

client    = AsyncWebClient(token=SLACK_BOT_TOKEN)
verifier  = SignatureVerifier(signing_secret=SLACK_SIGNING_SECRET)

# щоб не дублювати ті самі події
processed_event_ids = TTLCache(maxsize=1000, ttl=60)

# ──────────────────────────────────────────────────────────────────────────────
# HTTP handler for /slack/events
# ──────────────────────────────────────────────────────────────────────────────
async def handle_event(req: Request):
    body = await req.body()

    # перевірка підпису Slack
    if not verifier.is_valid_request(body, req.headers):
        logger.warning("Invalid Slack signature")
        return {"error": "invalid signature"}

    payload = await req.json()

    # URL verification
    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload["challenge"]})

    event = payload.get("event", {})
    event_id = payload.get("event_id")

    # дублі
    if event_id in processed_event_ids:
        logger.info("Skip duplicated event_id=%s", event_id)
        return {"ok": True}
    processed_event_ids[event_id] = True

    # ігноруємо ботів
    if event.get("bot_id") is not None:
        return {"ok": True}

    if event.get("type") in ("app_mention",) or event.get("channel_type") == "im":
        user_text = event.get("text", "")
        channel   = event.get("channel")
        user_id   = event.get("user", "default_user")
        logger.info("✉️ Slack-запит від користувача %s: %s", user_id, user_text)

        # фонова обробка, Slack миттєво отримає 200 OK
        asyncio.create_task(handle_user_query(user_text, channel, user_id))

    return {"ok": True}

# ──────────────────────────────────────────────────────────────────────────────
# Background worker
# ──────────────────────────────────────────────────────────────────────────────
async def handle_user_query(user_text: str, channel: str, user_id: str):
    try:
        # передаємо user_id у process_slack_message (важливо!)
        response = await asyncio.to_thread(process_slack_message, user_text, semantic_map, user_id)
    except Exception as e:
        logger.exception("❌ Error while processing Slack message")
        response = f"❌ Помилка: {str(e)}"

    try:
        await client.chat_postMessage(channel=channel, text=response)
    except Exception:
        logger.exception("❌ Failed to post message to Slack")
