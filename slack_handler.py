import os
import json
import logging
import asyncio  # ⬅️ Додано для запуску фонового завдання
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.signature import SignatureVerifier
from fastapi import Request
from dotenv import load_dotenv
from analytics import process_slack_message
from semantic_map import semantic_map
from fastapi.responses import JSONResponse
from cachetools import TTLCache  # ⬅️ Для запобігання дублюванню подій

# 🔐 Завантаження .env-змінних
load_dotenv()
logging.basicConfig(level=logging.INFO)

# 🔗 Slack API токени
slack_token = os.getenv("SLACK_BOT_TOKEN")
signing_secret = os.getenv("SLACK_SIGNING_SECRET")

client = AsyncWebClient(token=slack_token)
verifier = SignatureVerifier(signing_secret=signing_secret)

# 🛡 Кеш для унікальних event_id (щоб не дублювати події)
processed_event_ids = TTLCache(maxsize=1000, ttl=60)


# 📬 Обробка Slack подій
async def handle_event(req: Request):
    body = await req.body()

    # ✅ Перевірка підпису запиту
    if not verifier.is_valid_request(body, req.headers):
        return {"error": "invalid signature"}

    payload = await req.json()

    # ⚙️ Slack URL Verification (challenge)
    if payload.get("type") == "url_verification":
        return JSONResponse(content={"challenge": payload["challenge"]})

    event = payload.get("event", {})
    event_id = payload.get("event_id")

    # 🛡 Уникнення дублювання Slack-подій
    if event_id in processed_event_ids:
        logging.info(f"⏩ Подія {event_id} вже оброблена — ігноруємо.")
        return {"ok": True}
    processed_event_ids[event_id] = True  # Кешуємо ID

    # 🔄 Ігнор повідомлень від ботів
    if event.get("bot_id") is not None:
        return {"ok": True}

    if event.get("type") == "app_mention" or event.get("channel_type") == "im":
        user_text = event.get("text", "")
        channel = event.get("channel")
        user_id = event.get("user", "default_user")  # ⭐ ДОДАНО: отримуємо user_id

        logging.info(f"✉️ Slack-запит від користувача {user_id}: {user_text}")

        # 🧠 Запускаємо фонову обробку запиту з user_id
        asyncio.create_task(handle_user_query(user_text, channel, user_id))

    return {"ok": True}  # ⚡ Slack отримає миттєву відповідь


# 🧠 Фонова обробка повідомлення
async def handle_user_query(user_text: str, channel: str, user_id: str):  # ⭐ ДОДАНО: user_id параметр
    try:
        # ⭐ ВИПРАВЛЕНО: передаємо user_id до функції
        response = await asyncio.to_thread(process_slack_message, user_text, semantic_map, user_id)
    except Exception as e:
        logging.exception("❌ Помилка при обробці запиту")
        response = f"❌ Помилка: {str(e)}"

    await client.chat_postMessage(channel=channel, text=response)