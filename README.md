# 🌦️ Weather Pipeline — Industry-Grade Data Engineering

An industry-grade ETL pipeline that ingests real-time weather data for **500 Indian cities** every 5 minutes from the [Open-Meteo API](https://open-meteo.com/), processes it through validation, transformation, and anomaly detection layers, and stores it in PostgreSQL — orchestrated by **Prefect**, containerized with **Docker**, and tested with **pytest**.

---

## 🏗️ Architecture

```
Open-Meteo API → Extract (Batched) → Validate (Pydantic) → Load (Raw → Staging → Fact)
                                                           ↓
                                                    Aggregate (Daily/Weekly/Monthly)
                                                           ↓
                                                    Anomaly Detection (Z-Score)
                                                           ↓
                                                    Lineage & Monitoring
```

**Pipeline Flow:**
1. **Extract** — Fetch current weather for 500 cities in 10 batched API calls (50 cities/batch)
2. **Validate** — Pydantic models enforce data quality (temperature, windspeed, WMO codes)
3. **Load** — Insert raw data, upsert staging and fact tables (deduplication via unique constraints)
4. **Transform** — Compute daily/weekly/monthly aggregations, moving averages, trend analysis
5. **Detect** — Z-score anomaly detection for temperature and windspeed
6. **Track** — Full data lineage (source → target) and pipeline run monitoring with alerting

---

## 📂 Project Structure

```
weather-pipeline/
├── src/
│   ├── config/          # Settings (Pydantic) + 500 Indian cities dataset
│   ├── models/          # SQLAlchemy models (10 tables)
│   ├── extract/         # API client with batching & retries
│   ├── load/            # Bulk insert, upsert, dimension loading
│   ├── transform/       # Validators, aggregations, anomaly, trends
│   ├── lineage/         # Data lineage tracking
│   ├── monitoring/      # Pipeline health & alerting
│   └── utils/           # Structured logging (loguru)
├── flows/               # Prefect flow + tasks + schedule
├── alembic/             # Database migrations
├── tests/               # pytest test suite
├── Dockerfile           # Multi-stage build
├── docker-compose.yml   # PostgreSQL + pipeline
└── .github/workflows/   # CI/CD pipeline
```

---

## 🚀 Quick Start

### Prerequisites
- **Docker** & **Docker Compose**
- **Python 3.11+** (for local development)
- **Git**

### Option 1: Docker (Recommended)

```bash
# Clone the repository
git clone https://github.com/notr0hit/weather-pipeline.git
cd weather-pipeline

# Create .env file
cp .env.example .env

# Start everything (PostgreSQL + Pipeline)
docker-compose up --build -d

# View logs
docker-compose logs -f pipeline
```

### Option 2: Local Development

```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Set up PostgreSQL (Docker)
docker run -d --name weather-postgres \
  -e POSTGRES_USER=weather_user \
  -e POSTGRES_PASSWORD=weather_pass \
  -e POSTGRES_DB=weather_db \
  -p 5432:5432 \
  postgres:16-alpine

# Create .env
cp .env.example .env

# Run the pipeline
python -m flows.weather_flow
```

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src --cov-report=term-missing

# Run specific test file
pytest tests/test_validators.py -v
```

---

## 📊 Database Schema

| Table | Layer | Purpose |
|-------|-------|---------|
| `raw_weather_data` | Raw | Raw API JSON responses |
| `stg_weather_readings` | Staging | Validated & cleaned readings |
| `dim_cities` | Dimension | City master data (500 cities) |
| `fact_weather` | Fact | Core weather fact table |
| `agg_daily_weather` | Aggregation | Daily min/max/avg per city |
| `agg_weekly_weather` | Aggregation | Weekly rollups |
| `agg_monthly_weather` | Aggregation | Monthly rollups |
| `weather_anomalies` | Analytics | Z-score flagged anomalies |
| `data_lineage` | Lineage | Source-to-target tracking |
| `pipeline_runs` | Monitoring | Run metadata & status |

---

## ⚙️ Configuration

All settings are managed via environment variables (`.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port |
| `BATCH_SIZE` | `50` | Cities per API batch |
| `FETCH_INTERVAL_MINUTES` | `5` | Pipeline run frequency |
| `ANOMALY_Z_THRESHOLD` | `2.0` | Z-score threshold |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `ALERT_ON_FAILURE` | `true` | Enable failure alerts |

---

## 🔧 Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11 |
| Orchestration | Prefect 3.x |
| Database | PostgreSQL 16 |
| ORM | SQLAlchemy 2.x |
| Migrations | Alembic |
| Validation | Pydantic 2.x |
| HTTP Client | httpx |
| Retry Logic | tenacity |
| Logging | loguru |
| Testing | pytest |
| Linting | ruff |
| Containers | Docker + Docker Compose |
| CI/CD | GitHub Actions |

---

## 📝 License

MIT
