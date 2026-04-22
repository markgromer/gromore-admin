# Stage 1: Build React app
FROM node:22.12.0-bookworm-slim AS react-build
WORKDIR /build
COPY client-app/package.json client-app/package-lock.json ./
RUN node --version && npm --version && npm install --no-fund --no-audit
COPY client-app/ .
RUN npm run build

# Stage 2: Python app
FROM python:3.12-slim

WORKDIR /app

ENV PLAYWRIGHT_BROWSERS_PATH=/app/.playwright-browsers

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install chromium

COPY . .

# Copy React build output from stage 1
COPY --from=react-build /webapp/static/react/ ./webapp/static/react/

# Create data directories
RUN mkdir -p /data/database /data/reports

ENV DATABASE_PATH=/data/database/webapp.db
ENV FLASK_DEBUG=false
ENV PORT=10000

EXPOSE ${PORT}

CMD ["sh", "-c", "gunicorn webapp.app:app --bind 0.0.0.0:${PORT} --workers 2 --timeout 300"]
