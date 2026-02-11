import os
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from slack_handler import handle_slack_event, handle_slack_interactive, verify_slack_signature

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/slack/events")
async def slack_events(request: Request):
    raw_body = await request.body()

    # Slack signature verification
    if not verify_slack_signature(
        signing_secret=os.environ.get("SLACK_SIGNING_SECRET", ""),
        raw_body=raw_body,
        headers=request.headers,
    ):
        return PlainTextResponse("invalid signature", status_code=401)

    payload = json.loads(raw_body.decode("utf-8"))

    # URL verification challenge
    if payload.get("type") == "url_verification":
        return JSONResponse({"challenge": payload.get("challenge")})

    # Events callback
    await handle_slack_event(payload)
    return JSONResponse({"ok": True})

@app.post("/slack/interactive")
async def slack_interactive(request: Request):
    raw_body = await request.body()

    # Slack signature verification
    if not verify_slack_signature(
        signing_secret=os.environ.get("SLACK_SIGNING_SECRET", ""),
        raw_body=raw_body,
        headers=request.headers,
    ):
        return PlainTextResponse("invalid signature", status_code=401)

    # Slack sends as x-www-form-urlencoded: payload=<json>
    form = await request.form()
    payload_str = form.get("payload")
    if not payload_str:
        return PlainTextResponse("missing payload", status_code=400)

    payload = json.loads(payload_str)
    await handle_slack_interactive(payload)

    # Respond quickly to Slack
    return JSONResponse({"ok": True})
