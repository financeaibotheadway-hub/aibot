# entrypoint.py
import os
import re

MODE = os.getenv("BOT_MODE", "prod").lower()

if MODE == "dev":
    # DEV: Socket Mode (Colab / local)
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    from slack_handler import process_slack_message  # ✅ ЄДИНА логіка

    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    def _strip_mention(text: str) -> str:
        if not text:
            return ""
        return re.sub(r"^<@[\w]+>\s*", "", text).strip()

    @app.event("app_mention")
    def handle_mention(event, say):
        raw_text = event.get("text", "")
        text = _strip_mention(raw_text)

        response = process_slack_message(
            text=text,
            user_id=event.get("user", "slack"),
        )
        say(response)

    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

else:
    # PROD: FastAPI (Slack Events API)
    from main import app  # FastAPI app
