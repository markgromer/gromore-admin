FROM python:3.12-slim

# Install Node.js for React build
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Build the React client app into webapp/static/react/
RUN cd client-app && npm ci && npm run build

# Create data directories
RUN mkdir -p /data/database /data/reports

ENV DATABASE_PATH=/data/database/webapp.db
ENV FLASK_DEBUG=false
ENV PORT=10000

EXPOSE ${PORT}

CMD ["sh", "-c", "gunicorn webapp.app:app --bind 0.0.0.0:${PORT} --workers 2 --timeout 300"]
