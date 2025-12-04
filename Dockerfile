FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy requirements first
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Make ai_core importable
ENV PYTHONPATH="/app"

# Cloud Run gives port in $PORT
ENV PORT=8080

# Expose for local development
EXPOSE 8080

# Start FastAPI with uvicorn (main.py must contain: app = FastAPI())
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
