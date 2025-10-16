#!/usr/bin/env bash
set -e

# System deps for psycopg2
apt-get update >/dev/null
apt-get install -y gcc libpq-dev >/dev/null

# Install Python libs
pip install --no-cache-dir -r /work/app/requirements.txt

# Launch API (live reload helpful during dev)
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
