FROM python:3.11-slim

WORKDIR /app

RUN useradd -u 10001 -m -s /bin/sh appuser

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ ./schemas/
COPY enricher.py .

USER appuser

ENV PYTHONUNBUFFERED=1

CMD ["python", "enricher.py"]
