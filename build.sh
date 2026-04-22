#!/usr/bin/env bash
set -e

echo "=== Installing Python dependencies ==="
pip install -r requirements.txt

echo "=== Installing Playwright browser ==="
export PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-$PWD/.playwright-browsers}"
python -m playwright install --with-deps chromium

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

if [ ! -d "${PLAYWRIGHT_BROWSERS_PATH}" ]; then
	echo "FATAL: Playwright browser install missing at ${PLAYWRIGHT_BROWSERS_PATH}"
	exit 1
fi

ls -la webapp/static/react/
