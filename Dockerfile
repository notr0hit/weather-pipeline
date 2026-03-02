# ── Build Stage ────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install system dependencies for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ── Runtime Stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependency for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ src/
COPY flows/ flows/
COPY alembic/ alembic/
COPY alembic.ini .
COPY pyproject.toml .

# Create logs directory
RUN mkdir -p logs

# Set Python path so src is importable
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "from src.config.settings import get_settings; print('ok')" || exit 1

# Default command: run the Prefect flow
CMD ["python", "-m", "flows.weather_flow"]
