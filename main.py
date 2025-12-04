from fastapi import FastAPI, Request
from slack_handler import handle_event

app = FastAPI()


@app.get("/")
async def root():
    return {"status": "ok", "service": "aibot"}


@app.post("/slack/events")
async def slack_events(request: Request):
    """
    Entry-point для Slack Events (URL, який ти вказуєш у Slack App).
    """
    return await handle_event(request)
