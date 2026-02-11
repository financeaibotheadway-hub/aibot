import os
import time
import hmac
import json
import hashlib
import requests
from typing import Dict, Any, Optional, List

from memory_store import MemoryStore


SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET", "")

SLACK_API = "https://slack.com/api"


def verify_slack_signature(signing_secret: str, raw_body: bytes, headers) -> bool:
    """
    Verify Slack request signature:
    https://api.slack.com/authentication/verifying-requests-from-slack
    """
    if not signing_secret:
        return False

    timestamp = headers.get("X-Slack-Request-Timestamp")
    slack_sig = headers.get("X-Slack-Signature")
    if not timestamp or not slack_sig:
        return False

    # replay protection (5 min)
    try:
        if abs(time.time() - int(timestamp)) > 60 * 5:
            return False
    except Exception:
        return False

    base = f"v0:{timestamp}:".encode("utf-8") + raw_body
    digest = hmac.new(signing_secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    computed = f"v0={digest}"

    return hmac.compare_digest(computed, slack_sig)


def slack_api(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(
        f"{SLACK_API}/{method}",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json; charset=utf-8"},
        data=json.dumps(payload),
        timeout=30,
    )
    return r.json()


def build_feedback_blocks(answer_text: str, meta_value: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Slack block kit: answer + feedback buttons
    meta_value is embedded into button 'value' as JSON string.
    """
    value_str = json.dumps(meta_value, ensure_ascii=False)

    return [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": answer_text[:2900]},  # Slack safety
        },
        {"type": "divider"},
        {
            "type": "actions",
            "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "👍 Helpful"}, "action_id": "fb_good", "value": value_str},
                {"type": "button", "text": {"type": "plain_text", "text": "👎 Not helpful"}, "action_id": "fb_bad", "value": value_str},
                {"type": "button", "text": {"type": "plain_text", "text": "🧠 Bad context"}, "action_id": "fb_bad_context", "value": value_str},
                {"type": "button", "text": {"type": "plain_text", "text": "🧾 Bad SQL"}, "action_id": "fb_bad_sql", "value": value_str},
            ],
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "_This feedback trains bot memory (internal)._"}],
        },
    ]


async def handle_slack_event(payload: Dict[str, Any]) -> None:
    """
    Slack Events API handler.
    We handle:
    - event_callback -> message in channel/DM (exclude bot messages)
    """
    if payload.get("type") != "event_callback":
        return

    event = payload.get("event") or {}
    etype = event.get("type")

    if etype != "message":
        return

    # ignore bot messages and message edits
    if event.get("subtype") in ("bot_message", "message_changed", "message_deleted"):
        return

    user_id = event.get("user")
    channel_id = event.get("channel")
    text = (event.get("text") or "").strip()

    if not user_id or not channel_id or not text:
        return

    mem = MemoryStore()
    memory_context = mem.get_memory_context(text, limit=3)

    # ---- YOUR EXISTING "ANALYZE" ENTRYPOINT ----
    # Replace this call with your real function (Vertex AI, SQL agent etc).
    # We keep it as a single place so patch is stable.
    answer = mem.generate_answer_with_memory(user_text=text, memory_context=memory_context)

    # Store "candidate memory" immediately (so we have a stable record even before feedback)
    mem_id = mem.write_interaction(
        user_id=user_id,
        channel_id=channel_id,
        user_text=text,
        bot_text=answer,
        rating=None,
        feedback_type=None,
    )

    blocks = build_feedback_blocks(
        answer_text=answer,
        meta_value={
            "mem_id": mem_id,
            "user_id": user_id,
            "channel_id": channel_id,
        },
    )

    slack_api("chat.postMessage", {"channel": channel_id, "blocks": blocks})


async def handle_slack_interactive(payload: Dict[str, Any]) -> None:
    """
    Handles button clicks from Slack interactive messages.
    """
    actions = payload.get("actions") or []
    if not actions:
        return

    action = actions[0]
    action_id = action.get("action_id")
    value = action.get("value") or "{}"

    try:
        meta = json.loads(value)
    except Exception:
        meta = {}

    mem_id = meta.get("mem_id")
    user_id = payload.get("user", {}).get("id") or meta.get("user_id")
    channel_id = payload.get("channel", {}).get("id") or meta.get("channel_id")

    if not mem_id or not user_id or not channel_id:
        return

    # Map button -> rating/label
    if action_id == "fb_good":
        rating = 1
        feedback_type = "thumbs_up"
    elif action_id == "fb_bad":
        rating = 0
        feedback_type = "thumbs_down"
    elif action_id == "fb_bad_context":
        rating = 0
        feedback_type = "bad_context"
    elif action_id == "fb_bad_sql":
        rating = 0
        feedback_type = "bad_sql"
    else:
        return

    mem = MemoryStore()
    mem.write_feedback(mem_id=mem_id, user_id=user_id, channel_id=channel_id, rating=rating, feedback_type=feedback_type)

    # Optional: acknowledge to user (ephemeral)
    slack_api(
        "chat.postEphemeral",
        {
            "channel": channel_id,
            "user": user_id,
            "text": "✅ Feedback saved. Thanks!",
        },
    )
