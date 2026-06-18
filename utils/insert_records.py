import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "db"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "admin"),
}


def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def create_main_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_weather (
                    id                  SERIAL PRIMARY KEY,
                    date                TIMESTAMPTZ NOT NULL,
                    point               TEXT,
                    temperature_2m      FLOAT,
                    weather_code        INT,
                    apparent_temperature FLOAT,
                    precipitation       FLOAT,
                    relative_humidity_2m FLOAT,
                    dew_point_2m        FLOAT,
                    surface_pressure    FLOAT,
                    wind_speed_10m      FLOAT,
                    wind_direction_10m  FLOAT
                );
            """)


def create_processed_data_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hourly_weather_processed (
                    id          SERIAL PRIMARY KEY,
                    date        TIMESTAMPTZ NOT NULL,
                    hour        INT,
                    day_of_week INT,
                    day_of_year INT,
                    month       INT,
                    temperature_2m               FLOAT,
                    weather_code                 FLOAT,
                    apparent_temperature         FLOAT,
                    precipitation                FLOAT,
                    wind_speed_10m               FLOAT,
                    wind_direction_10m           FLOAT,
                    relative_humidity_2m         FLOAT,
                    dew_point_2m                 FLOAT,
                    surface_pressure             FLOAT,
                    temperature_2m_lag_1         FLOAT,
                    temperature_2m_lag_3         FLOAT,
                    temperature_2m_lag_6         FLOAT,
                    temperature_2m_lag_12        FLOAT,
                    temperature_2m_lag_24        FLOAT,
                    temperature_2m_roll_mean_6   FLOAT,
                    temperature_2m_roll_std_6    FLOAT,
                    apparent_temperature_lag_1         FLOAT,
                    apparent_temperature_lag_3         FLOAT,
                    apparent_temperature_lag_6         FLOAT,
                    apparent_temperature_lag_12        FLOAT,
                    apparent_temperature_lag_24        FLOAT,
                    apparent_temperature_roll_mean_6   FLOAT,
                    apparent_temperature_roll_std_6    FLOAT,
                    precipitation_lag_1         FLOAT,
                    precipitation_lag_3         FLOAT,
                    precipitation_lag_6         FLOAT,
                    precipitation_lag_12        FLOAT,
                    precipitation_lag_24        FLOAT,
                    precipitation_roll_mean_6   FLOAT,
                    precipitation_roll_std_6    FLOAT,
                    wind_speed_10m_lag_1         FLOAT,
                    wind_speed_10m_lag_3         FLOAT,
                    wind_speed_10m_lag_6         FLOAT,
                    wind_speed_10m_lag_12        FLOAT,
                    wind_speed_10m_lag_24        FLOAT,
                    wind_speed_10m_roll_mean_6   FLOAT,
                    wind_speed_10m_roll_std_6    FLOAT,
                    wind_direction_10m_lag_1         FLOAT,
                    wind_direction_10m_lag_3         FLOAT,
                    wind_direction_10m_lag_6         FLOAT,
                    wind_direction_10m_lag_12        FLOAT,
                    wind_direction_10m_lag_24        FLOAT,
                    wind_direction_10m_roll_mean_6   FLOAT,
                    wind_direction_10m_roll_std_6    FLOAT,
                    relative_humidity_2m_lag_1         FLOAT,
                    relative_humidity_2m_lag_3         FLOAT,
                    relative_humidity_2m_lag_6         FLOAT,
                    relative_humidity_2m_lag_12        FLOAT,
                    relative_humidity_2m_lag_24        FLOAT,
                    relative_humidity_2m_roll_mean_6   FLOAT,
                    relative_humidity_2m_roll_std_6    FLOAT
                );
            """)


def create_predictions_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    id                  SERIAL PRIMARY KEY,
                    prediction_date     TIMESTAMPTZ NOT NULL,
                    forecast_horizon    INT NOT NULL,
                    temperature_2m      FLOAT,
                    weather_code        FLOAT,
                    apparent_temperature FLOAT,
                    precipitation       FLOAT,
                    wind_speed_10m      FLOAT,
                    wind_direction_10m  FLOAT,
                    relative_humidity_2m FLOAT,
                    created_at          TIMESTAMPTZ DEFAULT NOW()
                );
            """)


def create_model_metrics_table(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS model_metrics (
                    id                SERIAL PRIMARY KEY,
                    evaluation_date   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    model_path        TEXT NOT NULL,
                    target_column     TEXT NOT NULL,
                    MAE               FLOAT,
                    RMSE              FLOAT,
                    R2                FLOAT,
                    retrained         BOOLEAN DEFAULT FALSE
                );
            """)


def create_all_tables(conn):
    create_main_table(conn)
    create_processed_data_table(conn)
    create_predictions_table(conn)
    create_model_metrics_table(conn)


def insert_raw_weather(conn, point_name: str, df: pd.DataFrame):
    query = """
        INSERT INTO hourly_weather (
            date, point, temperature_2m, weather_code,
            apparent_temperature, precipitation,
            relative_humidity_2m, dew_point_2m,
            surface_pressure, wind_speed_10m, wind_direction_10m
        ) VALUES %s
    """
    records = [
        (
            row["date"], point_name,
            row["temperature_2m"], int(row["weather_code"]),
            row["apparent_temperature"], row["precipitation"],
            row["relative_humidity_2m"], row["dew_point_2m"],
            row["surface_pressure"], row["wind_speed_10m"],
            row["wind_direction_10m"],
        )
        for _, row in df.iterrows()
    ]
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records)


def insert_predictions(conn, predictions: pd.DataFrame):
    query = """
        INSERT INTO predictions (
            prediction_date, forecast_horizon,
            temperature_2m, weather_code,
            apparent_temperature, precipitation,
            wind_speed_10m, wind_direction_10m,
            relative_humidity_2m
        ) VALUES %s
    """
    records = [
        (
            row["prediction_date"], int(row["forecast_horizon"]),
            row.get("temperature_2m"),
            row.get("weather_code"),
            row.get("apparent_temperature"),
            row.get("precipitation"),
            row.get("wind_speed_10m"),
            row.get("wind_direction_10m"),
            row.get("relative_humidity_2m"),
        )
        for _, row in predictions.iterrows()
    ]
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records)


def insert_model_metrics(conn, model_path: str, metrics_df: pd.DataFrame, retrained: bool = False):
    query = """
        INSERT INTO model_metrics (
            evaluation_date, model_path, target_column, MAE, RMSE, R2, retrained
        ) VALUES %s
    """
    records = [
        (row.get("evaluation_date", "NOW()"), model_path, col,
         row.get("MAE"), row.get("RMSE"), row.get("R2"), retrained)
        for col, row in metrics_df.iterrows()
    ]
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records)
