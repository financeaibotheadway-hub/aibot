# -*- coding: utf-8 -*-
import os
import logging
import asyncio
import re

from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import JSONResponse

from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.signature import SignatureVerifier
from cachetools import TTLCache

# Імпорт вашої аналітики (залишаємо як є)
from analytics import run_analysis
from semantic_map import semantic_map

# ──────────────────────────────────────────────────────────────────────────────
# SHARED MESSAGE PIPELINE
# ──────────────────────────────────────────────────────────────────────────────
def process_slack_message(
    text: str,
    user_id: str = "slack",
):
    """
    ЄДИНА точка входу для генерації відповіді бота.
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
    logger.error("ERROR: Missing SLACK_BOT_TOKEN or SLACK_SIGNING_SECRET in env")

client   = AsyncWebClient(token=SLACK_BOT_TOKEN)
verifier = SignatureVerifier(signing_secret=SLACK_SIGNING_SECRET)

# Кеш для дедуплікації подій (Slack може надсилати повтори)
processed_event_ids = TTLCache(maxsize=2000, ttl=120)

# Кеш для DM каналів (user_id -> dm_channel_id)
dm_channel_cache = TTLCache(maxsize=5000, ttl=24 * 3600)


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────
async def _get_dm_channel_id(user_id: str) -> str:
    """
    Отримує ID особистого чату (DM) з користувачем.
    """
    if user_id in dm_channel_cache:
        return dm_channel_cache[user_id]

    try:
        resp = await client.conversations_open(users=user_id)
        dm_channel_id = resp["channel"]["id"]
        dm_channel_cache[user_id] = dm_channel_id
        return dm_channel_id
    except Exception as e:
        logger.error(f"Failed to open DM with {user_id}: {e}")
        return None

def _strip_bot_mention(text: str) -> str:
    """Видаляє згадку бота (@BotName) з тексту повідомлення."""
    if not text:
        return text

    if SLACK_BOT_USER_ID:
        # Видаляємо конкретний ID бота
        text = re.sub(rf"<@{re.escape(SLACK_BOT_USER_ID)}>\s*", "", text)
    else:
        # Фоллбек: видаляємо будь-яку згадку на початку
        text = re.sub(r"^<@[\w]+>\s*", "", text)

    return text.strip()

async def _send_ephemeral_ack(channel: str, user_id: str, text: str) -> None:
    """
    Відправляє 'примарне' повідомлення, яке бачить тільки користувач у каналі.
    """
    try:
        await client.chat_postEphemeral(
            channel=channel,
            user=user_id,
            text=text,
        )
    except Exception as e:
        logger.error(f"Failed to post ephemeral ack: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# CORE LOGIC: RESPONDER
# ──────────────────────────────────────────────────────────────────────────────
async def _respond_async(
    user_text: str,
    source_channel: str,
    user_id: str,
):
    """
    Логіка відповіді:
    1. Генеруємо відповідь (run_analysis).
    2. Якщо джерело - Канал: пишемо результат в DM, а в канал кидаємо приховане повідомлення.
    3. Якщо джерело - DM: просто пишемо результат туди.
    """
    # 1. Генерація відповіді
    try:
        response_text = await asyncio.to_thread(
            process_slack_message,
            text=user_text,
            user_id=user_id,
        )
    except Exception as e:
        logger.exception("Error in run_analysis()")
        response_text = f"❌ Виникла помилка при обробці запиту: {str(e)}"

    # 2. Визначаємо, куди писати
    # У Slack ID DM-каналів завжди починаються на 'D'. Публічні 'C', Приватні 'G'.
    is_source_dm = source_channel.startswith("D")

    # 3. Відправка основної відповіді
    target_dm_id = await _get_dm_channel_id(user_id)
    
    if not target_dm_id:
        # Якщо не вдалося відкрити DM, пробуємо відповісти в той же канал як фоллбек
        logger.warning(f"Could not open DM for {user_id}, replying in source channel.")
        await client.chat_postMessage(channel=source_channel, text=response_text)
        return

    # Відправляємо відповідь в особисті (завжди)
    # Якщо користувач написав у DM, source_channel == target_dm_id, тому це спрацює коректно
    try:
        await client.chat_postMessage(channel=target_dm_id, text=response_text)
    except Exception as e:
        logger.error(f"Failed to send DM response: {e}")

    # 4. Якщо запит був НЕ з особистих (а з каналу) -> повідомляємо в каналі
    if not is_source_dm and source_channel != target_dm_id:
        ack_msg = "📩 Відповів у особисті повідомлення (DM), щоб не засмічувати канал."
        await _send_ephemeral_ack(source_channel, user_id, ack_msg)


# ──────────────────────────────────────────────────────────────────────────────
# SLACK EVENTS HANDLER (FastAPI)
# ──────────────────────────────────────────────────────────────────────────────
async def handle_event(req: Request):
    """
    Обробник HTTP запитів від Slack Events API.
    """
    # 1. Retry check (Slack іноді дублює запити, якщо ми довго думаємо)
    if req.headers.get("X-Slack-Retry-Num"):
        return JSONResponse(content={"ok": True})

    raw_body = await req.body()

    # 2. Перевірка підпису (Security)
    try:
        if not verifier.is_valid_request(raw_body, dict(req.headers)):
            return JSONResponse(status_code=401, content={"error": "invalid signature"})
    except Exception:
        return JSONResponse(status_code=401, content={"error": "signature check failed"})

    try:
        payload = await req.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "bad json"})

    # 3. URL Verification (потрібно при налаштуванні бота)
    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload.get("challenge")})

    event = payload.get("event", {}) or {}
    event_id = payload.get("event_id") or event.get("client_msg_id")

    # 4. Дедуплікація
    if event_id and event_id in processed_event_ids:
        return JSONResponse(content={"ok": True})
    if event_id:
        processed_event_ids[event_id] = True

    # 5. Ігноруємо повідомлення від ботів (щоб не було циклів)
    if event.get("bot_id") is not None:
        return JSONResponse(content={"ok": True})

    evt_type = event.get("type")
    channel_id = event.get("channel")
    user_id = event.get("user")

    # 6. Обробка: app_mention (у каналах) АБО message (у DM)
    # У DM 'type' події часто просто 'message', але channel_type='im'
    is_app_mention = evt_type == "app_mention"
    is_dm_message = evt_type == "message" and event.get("channel_type") == "im"

    if is_app_mention or is_dm_message:
        raw_text = event.get("text", "")
        user_text = _strip_bot_mention(raw_text)

        logger.info(f"New task from {user_id} in {channel_id}: {user_text}")

        # Запускаємо асинхронну обробку, щоб не блокувати відповідь Слаку (200 OK)
        asyncio.create_task(
            _respond_async(
                user_text=user_text,
                source_channel=channel_id,
                user_id=user_id,
            )
        )

    return JSONResponse(content={"ok": True})
