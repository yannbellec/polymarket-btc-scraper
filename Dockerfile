FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/logs data/parquet

ENV LOG_LEVEL=INFO
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]
