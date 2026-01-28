Finance AI Bot (Slack) — README

Цей репозиторій містить Slack-бота, який приймає повідомлення в Slack, запускає аналітичну/семантичну бізнес‑логіку та повертає відповідь у Slack.

Основна бізнес‑логіка знаходиться в `analytics/analytics_core.py` (викликається через `analytics.run_analysis()`), а єдина точка входу для Slack-повідомлень — `process_slack_message()` у `slack_handler.py`.



1) Що вміє бот

Відповідає на запити користувачів у Slack (через **Events API** у PROD або **Socket Mode** у DEV).
Нормалізує текст повідомлення (прибирає mention бота).
Викликає аналітичний модуль (`analytics.run_analysis`) та повертає текст відповіді.
Підтримує мапу семантик/інтентів (`semantic_map.py`), яку можна оверрайдити в виклику `run_analysis`.



2) Структура проєкту (ключові файли)

`entrypoint.py` — стартовий файл, який обирає режим роботи:
`BOT_MODE=dev` → Socket Mode (локально/Colab)
`BOT_MODE=prod` → FastAPI (Cloud Run / Events API)
`main.py` — FastAPI застосунок (ендпойнт `/slack/events`)
`slack_handler.py` — обробник Slack Events API (валідація підпису, dedupe, маршрутизація, відповідь)
`analytics/` — бізнес‑логіка (включно з `analytics_core.py`)
`semantic_map.py` — мапа семантик/інтентів
`requirements.txt` — залежності
`Dockerfile`, `cloudbuild.yaml` — деплой (якщо використовується Cloud Run / Cloud Build)



3) Змінні оточення (ENV)

Slack (мінімум)
**PROD (Events API / FastAPI):**
`SLACK_BOT_TOKEN` — `xoxb-...`
`SLACK_SIGNING_SECRET` — Signing Secret (для валідації запитів)
`SLACK_BOT_USER_ID` — `U...` (опціонально, для коректного видалення mention)

**DEV (Socket Mode):**
`SLACK_BOT_TOKEN` — `xoxb-...`
`SLACK_APP_TOKEN` — `xapp-...` (App-Level Token з правом `connections:write`)
`BOT_MODE=dev`

GCP / BigQuery / Vertex (якщо використовується в `analytics`)
Типово в проєкті зустрічаються такі змінні:
`BIGQUERY_PROJECT`
`BQ_DATASET`
`BQ_REVENUE_TABLE`
`BQ_COST_TABLE`
`VERTEX_LOCATION` (наприклад, `europe-west1`)
`LOCAL_TZ` (наприклад, `Europe/Kyiv`)

Точний перелік залежить від реалізації `analytics.run_analysis()`.



4) Налаштування Slack App

Event Subscriptions (PROD / Events API)
Увімкни **Event Subscriptions**
Вкажи Request URL:
`https://<your-cloud-run-domain>/slack/events`
Підпишися на події:
`app_mention` (щоб бот реагував на згадку в каналі)
(опціонально) `message.im` або загалом DM-події, якщо бот має реагувати на DM

Socket Mode (DEV)
Увімкни **Socket Mode**
Створи App-Level Token (`xapp-...`) з scope: `connections:write`

OAuth Scopes (мінімальні рекомендації)
`app_mentions:read`
`chat:write`
`im:history` (якщо читає DM через Events)
`im:write` (якщо бот відповідає в DM)
`conversations:read` (інколи потрібно)
`conversations:write` (потрібно для `conversations.open`, якщо відкриває DM)

Після зміни scopes натисни **Reinstall App**.



5) Запуск локально (DEV / Socket Mode)

Варіант A: локально
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export BOT_MODE=dev
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_APP_TOKEN="xapp-..."

python entrypoint.py

Варіант B: Google Colab
Клонуй репозиторій
Встанови залежності
Встанови ENV змінні
Запусти `python entrypoint.py`



6) Запуск у PROD (FastAPI / Events API)

Локально (для тесту вебхуків)
export BOT_MODE=prod
export SLACK_BOT_TOKEN="xoxb-..."
export SLACK_SIGNING_SECRET="..."
uvicorn main:app --host 0.0.0.0 --port 8080

Далі прокинь URL назовні (наприклад ngrok) і встав його в Slack → Event Subscriptions.

Cloud Run (типовий сценарій)
Збираєш Docker-образ (`Dockerfile`)
Деплоїш у Cloud Run
В Slack вставляєш `Request URL` на `/slack/events`



7) Логіка обробки повідомлень (коротко)

PROD (`slack_handler.py`)
Валідація підпису Slack (`SLACK_SIGNING_SECRET`)
Dedupe (через `TTLCache`)
Ігнор бот-повідомлень
Тригери:
`app_mention` (виклик у каналі)
або `channel_type == "im"` (DM)
Виклик `process_slack_message(text, user_id)` → `run_analysis(...)`
Відправка відповіді в Slack (`chat.postMessage`)

DEV (`entrypoint.py`)
Slack Bolt App + Socket Mode
Хендлери подій (наприклад, `app_mention`)
Той самий виклик `process_slack_message(...)`



8) Troubleshooting

“Missing SLACK_BOT_TOKEN / SLACK_SIGNING_SECRET”
Перевір ENV та `.env`
У DEV режимі `SLACK_SIGNING_SECRET` не потрібен, у PROD — потрібен.

Бот не реагує на mention
Перевір подію `app_mention` у Event Subscriptions
Перевір scopes (`app_mentions:read`, `chat:write`)
Reinstall App

Socket Mode підключився, але повідомлення не приходять
Перевір, що увімкнено Socket Mode
Перевір `SLACK_APP_TOKEN` (xapp) і scope `connections:write`
Переконайся, що `BOT_MODE=dev`

Відповідь приходить “не туди”
У PROD відповідає `slack_handler.py`
У DEV відповідає `entrypoint.py`
Перевір, який режим реально запущений (лог/ENV `BOT_MODE`)



9) Швидкий чеклист для релізу

[ ] Slack App: scopes додані, app reinstalled
[ ] Event Subscriptions: Request URL валідний і підтверджений
[ ] PROD: `SLACK_SIGNING_SECRET` встановлений
[ ] PROD: Cloud Run має доступ до потрібних GCP ресурсів (BQ/Vertex)
[ ] DEV: Socket Mode enabled + `SLACK_APP_TOKEN` заданий



10) Контакти / підтримка

Якщо потрібна допомога — писати на finance.ai.bot.headway@gmail.com / twinslytics@gmail.com

11) GCP service account acceses - <img width="443" height="320" alt="image" src="https://github.com/user-attachments/assets/66719a35-296b-4d63-ab1e-8103076e70d0" />

