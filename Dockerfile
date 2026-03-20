FROM python:3.13-slim

WORKDIR /app

# Avoid caching Python bytecode, keep it cleaner
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install OS dependencies (if needed for TLS/local stuff)
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  nano \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

CMD ["sh", "-c", "while true; do python main.py --discord --mins 75 --output-json /app/boosts_hierarchy.json --config /app/config.json --discord-state /app/discord_state.json; sleep 900; done"]
