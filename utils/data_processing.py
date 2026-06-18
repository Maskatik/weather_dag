import pandas as pd
import numpy as np
import os

RAW_DATA_PATH = "/home/makos/dev/weather_dag/raw_data"

FORECAST_HORIZON = 24

BASE_TARGET_COLS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "relative_humidity_2m",
]


def engineer_features(df: pd.DataFrame, target_cols=None, horizon=FORECAST_HORIZON) -> pd.DataFrame:
    if target_cols is None:
        target_cols = BASE_TARGET_COLS
    df = df.copy().sort_values("date").reset_index(drop=True)

    df["hour"] = df["date"].dt.hour
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_year"] = df["date"].dt.dayofyear
    df["month"] = df["date"].dt.month

    for col in target_cols:
        for lag in [1, 3, 6, 12, 24]:
            df[f"{col}_lag_{lag}"] = df[col].shift(lag)
        df[f"{col}_roll_mean_6"] = df[col].shift(1).rolling(6).mean()
        df[f"{col}_roll_std_6"] = df[col].shift(1).rolling(6).std()

    for col in target_cols:
        df[f"target_{col}"] = (
            df[col].shift(-(horizon - 1))
            + df[col].shift(-horizon)
            + df[col].shift(-(horizon + 1))
        ) / 3

    df = df.dropna().reset_index(drop=True)
    return df


def engineer_features_for_prediction(df: pd.DataFrame, target_cols=None) -> pd.DataFrame:
    if target_cols is None:
        target_cols = BASE_TARGET_COLS
    df = df.copy().sort_values("date").reset_index(drop=True)

    df["hour"] = df["date"].dt.hour
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_year"] = df["date"].dt.dayofyear
    df["month"] = df["date"].dt.month

    for col in target_cols:
        for lag in [1, 3, 6, 12, 24]:
            df[f"{col}_lag_{lag}"] = df[col].shift(lag)
        df[f"{col}_roll_mean_6"] = df[col].shift(1).rolling(6).mean()
        df[f"{col}_roll_std_6"] = df[col].shift(1).rolling(6).std()

    df = df.dropna().reset_index(drop=True)
    return df


def get_feature_columns(df: pd.DataFrame, target_cols=None) -> list:
    if target_cols is None:
        target_cols = BASE_TARGET_COLS
    target_cols_full = [f"target_{c}" for c in target_cols]
    exclude = set(target_cols_full) | set(target_cols) | {"date"}
    return [c for c in df.columns if c not in exclude]


def load_latest_csv(path=RAW_DATA_PATH) -> pd.DataFrame:
    files = [f for f in os.listdir(path) if f.startswith("city_data_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError(f"No city_data CSV found in {path}")
    latest = sorted(files)[-1]
    df = pd.read_csv(os.path.join(path, latest))
    df["date"] = pd.to_datetime(df["date"])
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    df = df.sort_values("date").reset_index(drop=True)
    return df
