# -*- coding: utf-8 -*-
import os
import json
import logging
import asyncio
import re

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

SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_BOT_USER_ID    = os.getenv("SLACK_BOT_USER_ID")  # опційно (щоб чистіше прибирати згадки)

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    logger.error("Missing SLACK_BOT_TOKEN or SLACK_SIGNING_SECRET in env")
client   = AsyncWebClient(token=SLACK_BOT_TOKEN)
verifier = SignatureVerifier(signing_secret=SLACK_SIGNING_SECRET)

# щоб не дублювати ті самі події
processed_event_ids = TTLCache(maxsize=2000, ttl=120)  # 2 хв — ок для Slack ретраїв

# ──────────────────────────────────────────────────────────────────────────────
# helpers
# ──────────────────────────────────────────────────────────────────────────────
def _strip_bot_mention(text: str) -> str:
    if not text:
        return text
    # якщо знаємо ід бота — прибираємо саме його
    if SLACK_BOT_USER_ID:
        text = re.sub(rf"<@{re.escape(SLACK_BOT_USER_ID)}>\s*", "", text)
    else:
        # на крайній випадок: прибрати першу згадку на початку
        text = re.sub(r"^<@[\w]+>\s*", "", text)
    return text.strip()

# ──────────────────────────────────────────────────────────────────────────────
# HTTP handler for /slack/events
# ──────────────────────────────────────────────────────────────────────────────
async def handle_event(req: Request):
    # Slack інколи ретраїть однакову подію — відразу відповідаємо 200
    if req.headers.get("X-Slack-Retry-Num"):
        return JSONResponse(content={"ok": True})

    body_bytes = await req.body()

    # перевірка підпису Slack
    try:
        if not verifier.is_valid_request(body_bytes, dict(req.headers)):
            logger.warning("Invalid Slack signature")
            return JSONResponse(status_code=401, content={"error": "invalid signature"})
    except Exception:
        logger.exception("Signature verification failed")
        return JSONResponse(status_code=401, content={"error": "invalid signature"})

    try:
        payload = await req.json()
    except Exception:
        logger.exception("Bad JSON from Slack")
        return JSONResponse(status_code=400, content={"error": "bad json"})

    # URL verification (challenge)
    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload.get("challenge")})

    event = payload.get("event", {}) or {}
    event_id = payload.get("event_id") or event.get("client_msg_id")

    # дублі (ще один рівень захисту)
    if event_id and event_id in processed_event_ids:
        logger.info("Skip duplicated event_id=%s", event_id)
        return JSONResponse(content={"ok": True})
    if event_id:
        processed_event_ids[event_id] = True

    # ігноруємо ботів/системні
    if event.get("bot_id") is not None:
        return JSONResponse(content={"ok": True})

    evt_type = event.get("type")
    channel_type = event.get("channel_type")
    if evt_type in ("app_mention",) or channel_type == "im":
        raw_text = event.get("text", "")
        user_text = _strip_bot_mention(raw_text)
        channel   = event.get("channel")
        user_id   = event.get("user", "default_user")
        thread_ts = event.get("thread_ts") or event.get("ts")
        logger.info("✉️ Slack %s from %s: %s", evt_type, user_id, user_text)

        # фонова обробка, Slack миттєво отримає 200 OK
        asyncio.create_task(_respond_async(user_text, channel, user_id, thread_ts))

    return JSONResponse(content={"ok": True})

async def _respond_async(user_text: str, channel: str, user_id: str, thread_ts: str | None):
    try:
        response = await asyncio.to_thread(process_slack_message, user_text, semantic_map, user_id)
    except Exception as e:
        logger.exception("❌ Error while processing Slack message")
        response = f"❌ Помилка: {str(e)}"

    try:
        kwargs = {"channel": channel, "text": response}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        await client.chat_postMessage(**kwargs)
    except Exception:
        logger.exception("❌ Failed to post message to Slack")
