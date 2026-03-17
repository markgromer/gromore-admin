FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create data directories
RUN mkdir -p /data/database /data/reports

ENV DATABASE_PATH=/data/database/webapp.db
ENV FLASK_DEBUG=false
ENV PORT=10000

EXPOSE 10000

CMD ["gunicorn", "webapp.app:app", "--bind", "0.0.0.0:10000", "--workers", "2", "--timeout", "120"]
