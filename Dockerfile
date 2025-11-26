FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .


# ✅ Запускаємо main.py, який читає змінну PORT
CMD ["python", "main.py"]
