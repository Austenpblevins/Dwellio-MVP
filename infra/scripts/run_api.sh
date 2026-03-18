#!/usr/bin/env bash
set -euo pipefail

python3 -m uvicorn app.main:app --reload --host "${DWELLIO_API_HOST:-0.0.0.0}" --port "${DWELLIO_API_PORT:-8000}"

