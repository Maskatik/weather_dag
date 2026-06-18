# Weather DAG

Hourly weather data pipeline on Apache Airflow with ML-based forecasting.

## Architecture

- **Airflow 3.1** — DAG orchestration
- **PostgreSQL** — data storage
- **XGBoost** — 24-hour weather forecasting

## Project Structure

```
.
├── dags/                  # Airflow DAGs
│   └── weather_pipeline.py
├── utils/                 # Shared utilities
│   ├── api_calls.py       # Open-Meteo API client
│   ├── data_processing.py # Feature engineering
│   ├── db_queries.py      # Database read functions
│   ├── insert_records.py  # Database write functions
│   └── model_utils.py     # ML model training/evaluation
├── config/
│   └── config.yaml        # App settings
├── Dockerfile             # Custom Airflow image
├── docker-compose.yaml    # Full stack deployment
└── requirements.txt       # Python dependencies
```

## Quick Start

### 1. Set up environment

```bash
cp .env.example .env
# Edit .env — replace placeholder values (API_KEY, passwords, fernet_key, etc.)
```

### 2. Run with Docker

```bash
docker compose up -d
```

Airflow UI will be available at `http://localhost:8080` (login: `airflow` / `airflow`).

### 3. Development without Docker

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `Weather_Pipeline` | Hourly | Extracts weather data → retrains model if needed → generates 24h forecast |

## Environment Variables

All secrets are read from `.env`. See `.env.example` for the full list.
