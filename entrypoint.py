# entrypoint.py
# -*- coding: utf-8 -*-
import os
import re
import logging
from cachetools import TTLCache

MODE = os.getenv("BOT_MODE", "prod").lower()

# ─────────────────────────────────────────────────────────────
# DEV: Socket Mode (Colab / local)
# ─────────────────────────────────────────────────────────────
if MODE == "dev":
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    # ЄДИНА бізнес-логіка (та сама, що в PROD)
    from slack_handler import process_slack_message

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("socket_mode")

    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    # cache: user_id -> dm_channel_id
    dm_channel_cache = TTLCache(maxsize=5000, ttl=24 * 3600)

    def _strip_bot_mention(text: str) -> str:
        if not text:
            return ""
        # прибирає <@Uxxxxxx> на початку
        return re.sub(r"^<@[\w]+>\s*", "", text).strip()

    def _get_dm_channel_id(user_id: str) -> str:
        """
        Open / reuse DM channel with user
        Scopes needed: im:write, conversations:write
        """
        if user_id in dm_channel_cache:
            return dm_channel_cache[user_id]

        resp = app.client.conversations_open(users=user_id)
        dm_id = resp["channel"]["id"]
        dm_channel_cache[user_id] = dm_id
        return dm_id

    def _reply_in_dm_and_notify_ephemeral(
        user_id: str,
        user_text: str,
        source_channel: str | None = None,
        source_thread_ts: str | None = None,
        send_ephemeral: bool = False,
    ) -> None:
        """
        1) Sends bot answer to DM
        2) Tries to send ephemeral note in the SAME THREAD (if possible)
           Fallback: sends ephemeral in channel (no thread_ts)
        """
        # 1) run analysis
        try:
            response = process_slack_message(text=user_text, user_id=user_id)
        except Exception as e:
            logger.exception("Error in process_slack_message")
            response = f"❌ Помилка: {str(e)}"

        # 2) DM
        try:
            dm_channel = _get_dm_channel_id(user_id)
            app.client.chat_postMessage(channel=dm_channel, text=response)
        except Exception:
            logger.exception("Failed to post DM message")

        # 3) Ephemeral note (visible only to the user)
        if send_ephemeral and source_channel:
            base_payload = {
                "channel": source_channel,
                "user": user_id,
                "text": "✅ Відповів у DM.",
            }

            # 3.1) Try to post ephemeral in thread
            if source_thread_ts:
                try:
                    payload = dict(base_payload)
                    payload["thread_ts"] = source_thread_ts
                    app.client.chat_postEphemeral(**payload)
                    return
                except Exception as e:
                    # Часто Slack не показує ephemeral в thread або кидає invalid_arguments.
                    # Робимо fallback в канал без thread_ts.
                    logger.warning(
                        f"Ephemeral in thread failed, fallback to channel. Reason: {e}"
                    )

            # 3.2) Fallback: ephemeral in channel (no thread_ts)
            try:
                app.client.chat_postEphemeral(**base_payload)
            except Exception:
                logger.exception("Failed to post ephemeral message (fallback)")

    @app.event("app_mention")
    def handle_mention(event, logger):
        raw_text = event.get("text", "")
        text = _strip_bot_mention(raw_text)

        user_id = event.get("user")
        channel = event.get("channel")
        thread_ts = event.get("thread_ts") or event.get("ts")

        if not user_id or not text:
            return

        logger.info(f"mention from {user_id}: {text}")

        # Відповідь -> DM, в каналі -> ephemeral "відповів у DM"
        _reply_in_dm_and_notify_ephemeral(
            user_id=user_id,
            user_text=text,
            source_channel=channel,
            source_thread_ts=thread_ts,
            send_ephemeral=True,
        )

    @app.event("message")
    def handle_dm_messages(event, logger):
        """
        Handle direct messages to the bot (channel_type=im).
        Важливо: не реагуємо на message subtypes, щоб не ловити edits/joins/etc.
        """
        if event.get("channel_type") != "im":
            return
        if event.get("subtype") is not None:
            return

        user_id = event.get("user")
        text = (event.get("text") or "").strip()
        if not user_id or not text:
            return

        logger.info(f"dm from {user_id}: {text}")

        # Це вже DM — відповідаємо просто в DM (без ephemeral)
        _reply_in_dm_and_notify_ephemeral(
            user_id=user_id,
            user_text=text,
            source_channel=None,
            source_thread_ts=None,
            send_ephemeral=False,
        )

    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

# ─────────────────────────────────────────────────────────────
# PROD: FastAPI (Slack Events API / Cloud Run)
# ─────────────────────────────────────────────────────────────
else:
    from main import app  # noqa
