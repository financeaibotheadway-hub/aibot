# entrypoint.py
import os

MODE = os.getenv("BOT_MODE", "prod").lower()

if MODE == "dev":
    # DEV: Socket Mode (Colab / local)
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    from analytics.analytics_core import run_analysis
    from semantic_map import semantic_map

    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    @app.event("app_mention")
    def handle_mention(event, say):
        text = event.get("text", "")
        response = run_analysis(
            message=text,
            semantic_map_override=semantic_map,
            user_id=event.get("user", "slack"),
        )
        say(response)

    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()

else:
    # PROD: FastAPI (Slack Events API)
    from main import app  # FastAPI app
