# slack_handler.py
# -*- coding: utf-8 -*-

import os
import logging
import asyncio
import re
import json

from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import JSONResponse

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.signature import SignatureVerifier
from cachetools import TTLCache

# Імпорт аналітики
from analytics.analytics_core import run_analysis
# Імпорт пам'яті (для оновлення рейтингу при натисканні кнопки)
from memory_system import update_rating

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG & INIT
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("slack")

SLACK_BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")
SLACK_BOT_USER_ID    = os.getenv("SLACK_BOT_USER_ID")

if not SLACK_BOT_TOKEN or not SLACK_SIGNING_SECRET:
    logger.error("ERROR: Missing SLACK_BOT_TOKEN or SLACK_SIGNING_SECRET")

client   = AsyncWebClient(token=SLACK_BOT_TOKEN)
verifier = SignatureVerifier(signing_secret=SLACK_SIGNING_SECRET)

processed_event_ids = TTLCache(maxsize=2000, ttl=120)
dm_channel_cache = TTLCache(maxsize=5000, ttl=24 * 3600)

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
async def _get_dm_channel_id(user_id: str) -> str:
    if user_id in dm_channel_cache: return dm_channel_cache[user_id]
    try:
        resp = await client.conversations_open(users=user_id)
        dm_id = resp["channel"]["id"]
        dm_channel_cache[user_id] = dm_id
        return dm_id
    except Exception as e:
        logger.error(f"DM open fail: {e}")
        return None

def _strip_bot_mention(text: str) -> str:
    if not text: return ""
    if SLACK_BOT_USER_ID:
        text = re.sub(rf"<@{re.escape(SLACK_BOT_USER_ID)}>\s*", "", text)
    else:
        text = re.sub(r"^<@[\w]+>\s*", "", text)
    return text.strip()

def _get_feedback_blocks(text, query_id):
    """Генерує блоки Slack з кнопками"""
    # Slack Blocks Kit
    return [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text}
        },
        {
            "type": "actions",
            "block_id": f"feedback_{query_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👍 Good (Learn)"},
                    "style": "primary",
                    "value": str(query_id),
                    "action_id": "vote_good"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 Bad"},
                    "style": "danger",
                    "value": str(query_id),
                    "action_id": "vote_bad"
                }
            ]
        }
    ]

# ──────────────────────────────────────────────────────────────────────────────
# CORE LOGIC: RESPONDER
# ──────────────────────────────────────────────────────────────────────────────
async def _respond_async(user_text: str, source_channel: str, user_id: str):
    """
    Генерує відповідь (з кнопками, якщо це аналітика) і відправляє в DM.
    """
    try:
        # Викликаємо аналітику (вона повертає dict {text, query_id})
        result = await asyncio.to_thread(
            run_analysis,
            message=user_text,
            user_id=user_id
        )
        
        # Перевіряємо формат відповіді
        if isinstance(result, dict):
            response_text = result.get("text", "")
            query_id = result.get("query_id")
        else:
            response_text = str(result)
            query_id = None

    except Exception as e:
        logger.exception("Error in analysis")
        response_text = f"❌ Error processing request: {str(e)}"
        query_id = None

    # Визначаємо куди слати
    target_dm_id = await _get_dm_channel_id(user_id)
    if not target_dm_id:
        target_dm_id = source_channel # Fallback to channel

    is_source_dm = source_channel.startswith("D")

    # Формуємо блоки (текст + кнопки, якщо є ID)
    blocks = None
    if query_id:
        blocks = _get_feedback_blocks(response_text, query_id)

    # Відправка
    try:
        if blocks:
            await client.chat_postMessage(channel=target_dm_id, text=response_text, blocks=blocks)
        else:
            await client.chat_postMessage(channel=target_dm_id, text=response_text)
    except Exception as e:
        logger.error(f"Send Error: {e}")

    # Повідомлення в публічному каналі, якщо запит був звідти
    if not is_source_dm and source_channel != target_dm_id:
        try:
            await client.chat_postEphemeral(
                channel=source_channel,
                user=user_id,
                text="📩 Answer sent to DM."
            )
        except: pass

# ──────────────────────────────────────────────────────────────────────────────
# HANDLER: INTERACTIVE (BUTTON CLICKS)
# ──────────────────────────────────────────────────────────────────────────────
async def handle_interactive(req: Request, payload_str: str):
    """Обробляє натискання кнопок"""
    try:
        payload = json.loads(payload_str)
    except:
        return JSONResponse(status_code=400, content={"error": "bad payload"})

    actions = payload.get("actions", [])
    if not actions:
        return JSONResponse(content={"ok": True})

    action = actions[0]
    action_id = action.get("action_id")
    query_id = action.get("value")
    
    # Більш безпечне отримання полів через .get()
    user_data = payload.get("user", {})
    channel_data = payload.get("channel", {})
    message_data = payload.get("message", {})

    user_id = user_data.get("id")
    channel_id = channel_data.get("id")
    message_ts = message_data.get("ts")
    
    if not (channel_id and message_ts):
        logger.error("Missing channel_id or message_ts in interactive payload")
        return JSONResponse(status_code=400, content={"error": "missing context"})

    # 1. Оновлюємо рейтинг (це запустить навчання, якщо Good)
    rating = "good" if action_id == "vote_good" else "bad"
    
    # 🔥 ВИПРАВЛЕНО: Обробка помилок при запису в БД
    if query_id and query_id != "None":
        try:
            await asyncio.to_thread(update_rating, query_id, rating)
            logger.info(f"Rating updated for query {query_id}: {rating}")
        except Exception as e:
            # Логуємо помилку, але НЕ зупиняємо виконання, щоб інтерфейс Slack оновився
            logger.error(f"CRITICAL: Failed to update rating in DB: {e}", exc_info=True)
    else:
        logger.warning("Received interaction without valid query_id")

    # 2. Оновлюємо повідомлення (прибираємо кнопки, пишемо результат)
    footer = "✅ Thanks! I'll remember this." if rating == "good" else "❌ Thanks for feedback."
    
    # Беремо оригінальний текст (перший блок)
    original_blocks = message_data.get("blocks", [])
    new_blocks = []
    if original_blocks:
        new_blocks.append(original_blocks[0]) # Лишаємо контент
    
    new_blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": footer}]
    })
    
    try:
        await client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=new_blocks,
            text="Feedback received" # Fallback text
        )
    except Exception as e:
        logger.error(f"Update Msg Error: {e}")

    return JSONResponse(content={"ok": True})

# ──────────────────────────────────────────────────────────────────────────────
# HANDLER: EVENTS (MESSAGES)
# ──────────────────────────────────────────────────────────────────────────────
async def handle_event(req: Request):
    """Обробляє вхідні повідомлення"""
    if req.headers.get("X-Slack-Retry-Num"):
        return JSONResponse(content={"ok": True})

    try:
        body = await req.body()
        if not verifier.is_valid_request(body, dict(req.headers)):
            return JSONResponse(status_code=401, content={"error": "invalid signature"})
        
        payload = await req.json()
    except:
        return JSONResponse(status_code=400, content={"error": "bad request"})

    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload.get("challenge")})

    event = payload.get("event", {})
    if not event: return JSONResponse(content={"ok": True})

    # Дедуплікація
    evt_id = payload.get("event_id")
    if evt_id in processed_event_ids: return JSONResponse(content={"ok": True})
    processed_event_ids[evt_id] = True

    if event.get("bot_id"): return JSONResponse(content={"ok": True})

    # Типи подій
    is_mention = event.get("type") == "app_mention"
    is_dm = event.get("type") == "message" and event.get("channel_type") == "im"

    if is_mention or is_dm:
        text = _strip_bot_mention(event.get("text", ""))
        user = event.get("user")
        channel = event.get("channel")
        
        logger.info(f"Task from {user}: {text}")
        
        asyncio.create_task(
            _respond_async(text, channel, user)
        )

    return JSONResponse(content={"ok": True})
