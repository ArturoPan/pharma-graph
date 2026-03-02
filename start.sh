#!/usr/bin/env bash
# One command to start backend + frontend (FastAPI serves both at http://localhost:8000)
set -e
cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
  echo "Run: python -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

# Kill any process already on port 8000
lsof -ti :8000 | xargs kill -9 2>/dev/null || true

# Open browser after server is up (optional; works on macOS/Linux)
(sleep 2 && open "http://localhost:8000" 2>/dev/null || xdg-open "http://localhost:8000" 2>/dev/null || true) &

exec .venv/bin/uvicorn main:app --reload --port 8000
