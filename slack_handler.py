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
# Імпорт пам'яті
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

# ### НОВА ФУНКЦІЯ: Динамічне створення блоків ###
def _create_dynamic_blocks(text, query_id):
    """
    Створює масив блоків для Slack.
    Якщо текст довший за 3000 символів, він розбивається на кілька блоків.
    """
    SLACK_TEXT_BLOCK_LIMIT = 2900  # Ліміт тексту в одному блоці (з запасом)
    
    blocks = []
    
    # Якщо текст короткий, створюємо один блок
    if len(text) <= SLACK_TEXT_BLOCK_LIMIT:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": text}
        })
    # Якщо текст довгий, нарізаємо його на частини
    else:
        # Розбиваємо по параграфах (подвійний перенос рядка)
        paragraphs = text.split('\n\n')
        current_chunk = ""
        for p in paragraphs:
            # Якщо наступний параграф не влазить, зберігаємо поточний чанк
            if len(current_chunk) + len(p) + 2 > SLACK_TEXT_BLOCK_LIMIT:
                if current_chunk.strip():
                    blocks.append({
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": current_chunk.strip()}
                    })
                current_chunk = ""
            
            current_chunk += p + "\n\n"
        
        # Додаємо останній шматок, якщо він залишився
        if current_chunk.strip():
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": current_chunk.strip()}
            })

    # В кінці ЗАВЖДИ додаємо блок з кнопками, якщо є query_id
    if query_id:
        blocks.append({
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
        })
        
    return blocks

# ──────────────────────────────────────────────────────────────────────────────
# CORE LOGIC: RESPONDER
# ──────────────────────────────────────────────────────────────────────────────
async def _respond_async(user_text: str, source_channel: str, user_id: str):
    """
    Генерує відповідь і відправляє в DM, використовуючи динамічні блоки.
    """
    try:
        result = await asyncio.to_thread(
            run_analysis,
            message=user_text,
            user_id=user_id
        )
        
        if isinstance(result, dict):
            response_text = result.get("text", "An error occurred during analysis.")
            query_id = result.get("query_id")
        else:
            response_text = str(result)
            query_id = None

    except Exception as e:
        logger.exception("Error in run_analysis task")
        response_text = f"❌ An internal error occurred: {str(e)}"
        query_id = None

    target_dm_id = await _get_dm_channel_id(user_id)
    if not target_dm_id:
        target_dm_id = source_channel # Fallback to original channel

    is_source_dm = source_channel.startswith("D")

    # ### ОНОВЛЕНО: Використовуємо нову функцію для створення блоків ###
    blocks = _create_dynamic_blocks(response_text, query_id)

    try:
        # Відправляємо масив блоків. Slack сам їх відобразить.
        await client.chat_postMessage(
            channel=target_dm_id, 
            text=response_text[:3000], # Fallback text для нотифікацій
            blocks=blocks
        )
    except Exception as e:
        logger.error(f"Failed to send block response: {e}")
        # Якщо блоки не спрацювали, пробуємо відправити просто текст
        try:
            await client.chat_postMessage(channel=target_dm_id, text=response_text)
        except Exception as e2:
             logger.error(f"Failed to send plain text fallback: {e2}")

    # Повідомлення в публічному каналі, якщо запит був звідти
    if not is_source_dm and source_channel != target_dm_id:
        try:
            await client.chat_postEphemeral(
                channel=source_channel,
                user=user_id,
                text="📩 I've sent the answer to your Direct Messages."
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
    
    user_data = payload.get("user", {})
    channel_data = payload.get("channel", {})
    message_data = payload.get("message", {})

    channel_id = channel_data.get("id")
    message_ts = message_data.get("ts")
    
    if not (channel_id and message_ts):
        return JSONResponse(status_code=400, content={"error": "missing context"})

    rating = "good" if action_id == "vote_good" else "bad"
    
    if query_id and query_id != "None":
        # Запускаємо оновлення рейтингу у фоні
        asyncio.create_task(
            asyncio.to_thread(update_rating, query_id, rating)
        )
        logger.info(f"Scheduled rating update for {query_id}: {rating}")

    # Оновлення повідомлення в Slack
    footer = "✅ Thanks! I'll remember this." if rating == "good" else "❌ Thanks for feedback."
    
    original_blocks = message_data.get("blocks", [])
    new_blocks = []
    
    # Збираємо всі текстові блоки, крім останнього (де були кнопки)
    for block in original_blocks:
        if block.get("type") != "actions":
            new_blocks.append(block)
    
    # Додаємо новий футер
    new_blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": footer}]
    })
    
    try:
        await client.chat_update(
            channel=channel_id,
            ts=message_ts,
            blocks=new_blocks,
            text="Feedback received"
        )
    except Exception as e:
        logger.error(f"Update Msg Error: {e}")

    return JSONResponse(content={"ok": True})

# ──────────────────────────────────────────────────────────────────────────────
# HANDLER: EVENTS (MESSAGES)
# ──────────────────────────────────────────────────────────────────────────────
async def handle_event(req: Request):
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

    evt_id = payload.get("event_id")
    if evt_id in processed_event_ids: return JSONResponse(content={"ok": True})
    processed_event_ids[evt_id] = True

    if event.get("bot_id"): return JSONResponse(content={"ok": True})

    is_mention = event.get("type") == "app_mention"
    is_dm = event.get("type") == "message" and event.get("channel_type") == "im"

    if is_mention or is_dm:
        text = _strip_bot_mention(event.get("text", ""))
        user = event.get("user")
        channel = event.get("channel")

        if not user:
            logger.warning(f"Skipping event with no user. Event type: {event.get('type')}")
            return JSONResponse(content={"ok": True})
        
        logger.info(f"Task from {user}: {text}")
        
        asyncio.create_task(
            _respond_async(text, channel, user)
        )

    return JSONResponse(content={"ok": True})
