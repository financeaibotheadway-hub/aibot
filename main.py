import os
from fastapi import FastAPI, Request
from slack_handler import handle_event

app = FastAPI()


# HEALTHCHECK FOR CLOUD RUN
@app.get("/")
async def root():
    return {"status": "ok"}


@app.post("/slack/events")
async def slack_events(req: Request):
    return await handle_event(req)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
