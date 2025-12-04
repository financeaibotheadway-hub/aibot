from fastapi import FastAPI
from routers.slack_handler import router as slack_router

app = FastAPI()

# підключаємо роут Slack
app.include_router(slack_router)

@app.get("/")
async def root():
    return {"status": "ok", "service": "aibot"}
