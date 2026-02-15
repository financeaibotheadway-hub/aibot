from fastapi import FastAPI, Request, Form
from slack_handler import handle_event, handle_interactive

app = FastAPI()

@app.get("/")
async def root():
    return {"status": "ok", "service": "aibot"}

@app.post("/slack/events")
async def slack_events(request: Request):
    """
    Основний ендпоінт для Slack.
    Приймає і Events API (повідомлення), і Interactivity (кнопки).
    """
    # Slack надсилає Interactivity (кнопки) як Form Data у полі "payload"
    # Events API надсилає як JSON Body
    
    content_type = request.headers.get("content-type", "")
    
    if "application/x-www-form-urlencoded" in content_type:
        # Це натискання кнопки (Interactivity)
        form_data = await request.form()
        payload = form_data.get("payload")
        return await handle_interactive(request, payload)
    else:
        # Це звичайна подія (повідомлення)
        return await handle_event(request)
