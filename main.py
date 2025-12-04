from fastapi import FastAPI, Request
from slack_handler import handle_event

app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "aibot"}

@app.post("/slack/events")
async def slack_events(request: Request):
    """
    Slack Events API endpoint.
    """
    return await handle_event(request)
