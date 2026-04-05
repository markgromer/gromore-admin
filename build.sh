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
if [ ! -f "webapp/static/react/index.html" ]; then
	echo "FATAL: React build output missing at webapp/static/react/index.html"
	exit 1
fi

ls -la webapp/static/react/
