# entrypoint.py
import os
import re

MODE = os.getenv("BOT_MODE", "prod").lower()

# ─────────────────────────────────────────────────────────────
# DEV: Socket Mode (Colab / local)
# ─────────────────────────────────────────────────────────────
if MODE == "dev":
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    # ❗ ЄДИНА бізнес-логіка (та сама, що в PROD)
    from slack_handler import process_slack_message

    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    def _strip_bot_mention(text: str) -> str:
        """
        Повністю повторює логіку продового handler'а
        """
        if not text:
            return ""
        return re.sub(r"^<@[\w]+>\s*", "", text).strip()

    @app.event("app_mention")
    def handle_mention(event, say):
        raw_text = event.get("text", "")
        text = _strip_bot_mention(raw_text)

        user_id = event.get("user", "slack")
        thread_ts = event.get("thread_ts") or event.get("ts")

        # 🔴 КЛЮЧОВЕ: той самий виклик, що і в PROD
        response = process_slack_message(
            text=text,
            user_id=user_id,
        )

        # 🔴 ВІДПОВІДЬ У THREAD (як у проді)
        say(
            text=response,
            thread_ts=thread_ts,
        )

    # запуск Socket Mode
    SocketModeHandler(
        app,
        os.environ["SLACK_APP_TOKEN"]
    ).start()


# ─────────────────────────────────────────────────────────────
# PROD: FastAPI (Slack Events API / Cloud Run)
# ─────────────────────────────────────────────────────────────
else:
    from main import app  # FastAPI app (prod)
