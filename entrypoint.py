# entrypoint.py
# -*- coding: utf-8 -*-
import os
import re
import logging
from cachetools import TTLCache
from typing import Optional

MODE = os.getenv("BOT_MODE", "prod").lower()

# ─────────────────────────────────────────────────────────────
# DEV: Socket Mode (Colab / local)
# ─────────────────────────────────────────────────────────────
if MODE == "dev":
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler
    from slack_sdk.errors import SlackApiError

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
        return re.sub(r"^<@[\w]+>\s*", "", text).strip()

    def _get_dm_channel_id(user_id: str) -> str:
        """
        Open / reuse DM channel with user.
        Required scopes (Bot Token Scopes):
          - conversations:write   (required for conversations.open)
          - chat:write            (to send DM)
          - (sometimes) im:write  (depends on workspace policies / legacy)
        """
        if user_id in dm_channel_cache:
            return dm_channel_cache[user_id]

        resp = app.client.conversations_open(users=user_id)
        dm_id = resp["channel"]["id"]
        dm_channel_cache[user_id] = dm_id
        return dm_id

    def _post_ephemeral_notice(source_channel: str, user_id: str) -> None:
        """
        Ephemeral message is visible only to that user.
        IMPORTANT: Slack does NOT support ephemeral in threads (no thread_ts).
        """
        try:
            app.client.chat_postEphemeral(
                channel=source_channel,
                user=user_id,
                text="✅ Відповів у DM. Перевір приватні повідомлення з ботом."
            )
        except SlackApiError as e:
            logger.warning(
                f"chat_postEphemeral failed: {e.response.get('error')}"
            )
        except Exception:
            logger.exception("chat_postEphemeral failed (unexpected)")

    def _reply_in_dm_and_notify(
        user_id: str,
        user_text: str,
        source_channel: Optional[str] = None,
        notify_ephemeral: bool = False,
    ) -> None:
        """
        1) Run analysis
        2) Send answer to DM
        3) Optionally send ephemeral notice in the source channel (to the author only)
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
        except SlackApiError as e:
            # Це головне — тут ти побачиш missing_scope / not_allowed / etc
            logger.error(
                f"DM send failed: {e.response.get('error')} | needed: {e.response.get('needed')} | provided: {e.response.get('provided')}"
            )
        except Exception:
            logger.exception("Failed to post DM message (unexpected)")

        # 3) Ephemeral notice in channel (visible only to that user)
        if notify_ephemeral and source_channel:
            _post_ephemeral_notice(source_channel=source_channel, user_id=user_id)

    @app.event("app_mention")
    def handle_mention(event, logger):
        raw_text = event.get("text", "") or ""
        text = _strip_bot_mention(raw_text)

        user_id = event.get("user")
        channel = event.get("channel")

        if not user_id or not channel or not text:
            return

        logger.info(f"mention from {user_id} in {channel}: {text}")

        # Відповідь -> DM, в каналі -> ephemeral для автора
        _reply_in_dm_and_notify(
            user_id=user_id,
            user_text=text,
            source_channel=channel,
            notify_ephemeral=True,
        )

    @app.event("message")
    def handle_dm_messages(event, logger):
        """
        Handle direct messages to the bot (channel_type=im).
        Required scopes for receiving DM events:
          - im:history (for event subscriptions)
        Also ensure App Home -> Messages tab is enabled.
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

        # Це вже DM — відповідаємо в DM (без ephemeral)
        _reply_in_dm_and_notify(
            user_id=user_id,
            user_text=text,
            source_channel=None,
            notify_ephemeral=False,
        )

    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

# ─────────────────────────────────────────────────────────────
# PROD: FastAPI (Slack Events API / Cloud Run)
# ─────────────────────────────────────────────────────────────
else:
    from main import app  # noqa
