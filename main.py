import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Ваші імпорти
from analytics.analytics_core import run_analysis
from memory_manager import update_rating
from semantic_learner import learn_new_semantics

# Ініціалізація (ваша)
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN")

app = App(token=SLACK_BOT_TOKEN)

# --- ФУНКЦІЯ ГЕНЕРАЦІЇ КНОПОК ---
def get_feedback_blocks(text, query_id):
    return [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text}
        },
        {
            "type": "actions",
            "block_id": f"feedback_{query_id}",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👍 Good"},
                    "style": "primary",
                    "value": query_id,
                    "action_id": "vote_good"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 Bad SQL"},
                    "style": "danger",
                    "value": query_id,
                    "action_id": "vote_bad_sql"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "👎 Bad Context"},
                    "style": "danger",
                    "value": query_id,
                    "action_id": "vote_bad_context"
                }
            ]
        }
    ]

# --- ОБРОБКА ПОВІДОМЛЕНЬ ---
@app.message(".*")
def handle_message(message, say):
    user_query = message["text"]
    user_id = message["user"]
    
    # Викликаємо оновлену функцію (яка повертає dict)
    # Зверніть увагу: в analytics_core треба змінити run_analysis, 
    # щоб він викликав process_slack_message і повертав словник
    result = run_analysis(user_query, user_id=user_id)
    
    # Якщо result це просто рядок (старий код), відправляємо як є
    if isinstance(result, str):
        say(result)
        return

    # Якщо це словник з ID (новий код)
    response_text = result["text"]
    query_id = result.get("query_id")
    
    if query_id:
        blocks = get_feedback_blocks(response_text, query_id)
        say(blocks=blocks, text=response_text)
    else:
        say(response_text)

# --- ОБРОБКА КНОПОК ---

@app.action("vote_good")
def handle_vote_good(ack, body, client):
    ack()
    query_id = body["actions"][0]["value"]
    user_id = body["user"]["id"]
    
    # 1. Оновлюємо рейтинг в пам'яті
    record = update_rating(query_id, "good")
    
    # 2. Оновлюємо повідомлення в Slack (прибираємо кнопки)
    channel_id = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    original_text = body["message"]["blocks"][0]["text"]["text"]
    
    client.chat_update(
        channel=channel_id,
        ts=message_ts,
        text=original_text,
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": original_text}
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"✅ Фідбек прийнято (by <@{user_id}>). Вчуся..."}]
            }
        ]
    )
    
    # 3. Запускаємо навчання (Якщо є запис)
    if record:
        learn_new_semantics(record["query"], record["sql"])

@app.action("vote_bad_sql")
def handle_vote_bad_sql(ack, body, client):
    ack()
    query_id = body["actions"][0]["value"]
    update_rating(query_id, "bad_sql")
    
    # Оновлюємо UI
    client.chat_update(
        channel=body["channel"]["id"],
        ts=body["message"]["ts"],
        blocks=[
            {"type": "section", "text": {"type": "mrkdwn", "text": body["message"]["blocks"][0]["text"]["text"]}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "❌ Фідбек: Поганий SQL. Врахую."}]}
        ]
    )

@app.action("vote_bad_context")
def handle_vote_bad_context(ack, body, client):
    ack()
    query_id = body["actions"][0]["value"]
    update_rating(query_id, "bad_context")
    
    client.chat_update(
        channel=body["channel"]["id"],
        ts=body["message"]["ts"],
        blocks=[
            {"type": "section", "text": {"type": "mrkdwn", "text": body["message"]["blocks"][0]["text"]["text"]}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": "⚠️ Фідбек: Невірний контекст. Врахую."}]}
        ]
    )

if __name__ == "__main__":
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
