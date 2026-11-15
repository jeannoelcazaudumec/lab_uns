FROM python:3.11-slim

WORKDIR /app

RUN useradd -u 10001 -m -s /bin/sh appuser

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY server.py .
COPY test_client.py .

USER appuser

# Expose standard Modbus TCP port
EXPOSE 502

ENV PYTHONUNBUFFERED=1
ENV SIM_INTERVAL=2
ENV MODBUS_PORT=502

CMD ["python", "server.py"]
