import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}


def get_actuals_for_date(target_date: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT date, temperature_2m, weather_code,
               apparent_temperature, precipitation,
               wind_speed_10m, wind_direction_10m,
               relative_humidity_2m
        FROM hourly_weather
        WHERE date::date = %s::date AND point = 'city_center'
        ORDER BY date
    """
    df = pd.read_sql_query(query, conn, params=(target_date,))
    conn.close()
    return df


def get_predictions_for_date(target_date: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT prediction_date, temperature_2m, weather_code,
               apparent_temperature, precipitation,
               wind_speed_10m, wind_direction_10m,
               relative_humidity_2m
        FROM predictions
        WHERE prediction_date::date = %s::date
        ORDER BY prediction_date
    """
    df = pd.read_sql_query(query, conn, params=(target_date,))
    conn.close()
    return df


def get_latest_processed_data(lookback_hours: int = 168) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)
    query = """
        SELECT date, temperature_2m, weather_code,
               apparent_temperature, precipitation,
               wind_speed_10m, wind_direction_10m,
               relative_humidity_2m, dew_point_2m, surface_pressure
        FROM hourly_weather
        WHERE date >= %s AND point = 'city_center'
        ORDER BY date
    """
    df = pd.read_sql_query(query, conn, params=(cutoff,))
    conn.close()
    return df
