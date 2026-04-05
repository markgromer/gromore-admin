#!/usr/bin/env bash
set -e

echo "=== Installing Python dependencies ==="
pip install -r requirements.txt

echo "=== Building React client app ==="
cd client-app
npm ci
npm run build
cd ..

echo "=== Build complete ==="
ls -la webapp/static/react/ 2>/dev/null || echo "WARNING: React build output not found"
