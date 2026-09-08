#!/bin/bash
cd "$(dirname "$0")"
source .env 2>/dev/null || true
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
