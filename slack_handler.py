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

from analytics import run_analysis
from semantic_map import semantic_map

# ──────────────────────────────────────────────────────────────────────────────
# SHARED MESSAGE PIPELINE (DEV + PROD)
# ──────────────────────────────────────────────────────────────────────────────
def process_slack_message(
    text: str,
    user_id: str = "slack",
):
    """
    ЄДИНА точка входу для обробки Slack-повідомлення.
    Використовується і в PROD (FastAPI), і в DEV (Socket / Colab).
    """
    response = run_analysis(
        message=text,
        semantic_map_override=semantic_map,
        user_id=user_id,
    )
    return response

# ──────────────────────────────────────────────────────────────────────────────
# ENV / LOG
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("slack")

SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_BOT_USER_ID    = os.getenv("SLACK_BOT_USER_ID")

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    logger.error("Missing SLACK_BOT_TOKEN or SLACK_SIGNING_SECRET in env")

client   = AsyncWebClient(token=SLACK_BOT_TOKEN)
verifier = SignatureVerifier(signing_secret=SLACK_SIGNING_SECRET)

processed_event_ids = TTLCache(maxsize=2000, ttl=120)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _strip_bot_mention(text: str) -> str:
    if not text:
        return text

    if SLACK_BOT_USER_ID:
        text = re.sub(rf"<@{re.escape(SLACK_BOT_USER_ID)}>\s*", "", text)
    else:
        text = re.sub(r"^<@[\w]+>\s*", "", text)

    return text.strip()


# ──────────────────────────────────────────────────────────────────────────────
# Slack Events Handler
# ──────────────────────────────────────────────────────────────────────────────
async def handle_event(req: Request):

    if req.headers.get("X-Slack-Retry-Num"):
        return JSONResponse(content={"ok": True})

    raw_body = await req.body()

    try:
        if not verifier.is_valid_request(raw_body, dict(req.headers)):
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

    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload.get("challenge")})

    event = payload.get("event", {}) or {}
    event_id = payload.get("event_id") or event.get("client_msg_id")

    if event_id and event_id in processed_event_ids:
        return JSONResponse(content={"ok": True})
    if event_id:
        processed_event_ids[event_id] = True

    if event.get("bot_id") is not None:
        return JSONResponse(content={"ok": True})

    evt_type = event.get("type")
    channel_type = event.get("channel_type")

    if evt_type == "app_mention" or channel_type == "im":

        raw_text = event.get("text", "")
        user_text = _strip_bot_mention(raw_text)

        channel   = event.get("channel")
        user_id   = event.get("user", "default_user")
        thread_ts = event.get("thread_ts") or event.get("ts")

        logger.info(f"Slack message from {user_id}: {user_text}")

        asyncio.create_task(
            _respond_async(user_text, channel, user_id, thread_ts)
        )

    return JSONResponse(content={"ok": True})


# ──────────────────────────────────────────────────────────────────────────────
# Background Processor
# ──────────────────────────────────────────────────────────────────────────────
async def _respond_async(user_text: str, channel: str, user_id: str, thread_ts: str | None):

    try:
        response = await asyncio.to_thread(
        process_slack_message,
        text=user_text,
        user_id=user_id,
    )
    except Exception as e:
        logger.exception("Error in run_analysis()")
        response = f"❌ Помилка: {str(e)}"

    try:
        msg = {"channel": channel, "text": response}
        if thread_ts:
            msg["thread_ts"] = thread_ts

        await client.chat_postMessage(**msg)

    except Exception:
        logger.exception("Failed to post message to Slack")
