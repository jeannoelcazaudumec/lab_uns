FROM python:3.11-slim

WORKDIR /app

RUN useradd -u 10001 -m -s /bin/sh appuser

RUN pip install --no-cache-dir asyncua

COPY server.py /app/server.py

USER appuser

EXPOSE 4840

CMD ["python", "server.py"]
