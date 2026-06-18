import joblib
import pandas as pd
import numpy as np
import os
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.multioutput import MultiOutputRegressor
from xgboost import XGBRegressor

MODEL_DIR = "/opt/airflow/models"
MODEL_PATH = os.path.join(MODEL_DIR, "weather_forecaster_3h_avg.pkl")
RETRAIN_THRESHOLD_R2 = 0.5
FORECAST_HORIZON = 24

BASE_TARGET_COLS = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "relative_humidity_2m",
]
TARGET_COLS = [f"target_{c}" for c in BASE_TARGET_COLS]


def load_model(path=MODEL_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model not found at {path}")
    return joblib.load(path)


def save_model(model, path=MODEL_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump(model, path)
    print(f"Model saved to {path}")


def predict_next_24h(features_df: pd.DataFrame, feature_cols: list, model) -> pd.DataFrame:
    X = features_df[feature_cols].iloc[-FORECAST_HORIZON:]
    preds_array = model.predict(X)
    preds_df = pd.DataFrame(preds_array, columns=TARGET_COLS)
    base_dates = features_df["date"].iloc[-FORECAST_HORIZON:].values
    predictions = []
    for i, base_date in enumerate(base_dates):
        pred_date = pd.Timestamp(base_date) + pd.Timedelta(hours=FORECAST_HORIZON)
        row = {"prediction_date": pred_date, "forecast_horizon": FORECAST_HORIZON}
        for j, col in enumerate(BASE_TARGET_COLS):
            row[col] = float(preds_df.iloc[i, j])
        predictions.append(row)
    return pd.DataFrame(predictions)


def evaluate_predictions(actuals_df: pd.DataFrame, predictions_df: pd.DataFrame) -> pd.DataFrame:
    merged = pd.merge(
        actuals_df, predictions_df,
        left_on="date", right_on="prediction_date",
        suffixes=("_actual", "_pred"),
    )
    metrics = {}
    for col in BASE_TARGET_COLS:
        y_true = merged[f"{col}_actual"].values
        y_pred = merged[f"{col}_pred"].values
        if len(y_true) == 0:
            continue
        metrics[col] = {
            "MAE": mean_absolute_error(y_true, y_pred),
            "RMSE": np.sqrt(mean_squared_error(y_true, y_pred)),
            "R2": r2_score(y_true, y_pred),
        }
    metrics_df = pd.DataFrame(metrics).T
    metrics_df.index.name = "target_column"
    return metrics_df


def should_retrain(metrics_df: pd.DataFrame, threshold=RETRAIN_THRESHOLD_R2) -> bool:
    if "R2" not in metrics_df.columns:
        return False
    avg_r2 = metrics_df["R2"].mean()
    return avg_r2 < threshold


def train_new_model(X_train, y_train):
    base_model = XGBRegressor(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42,
    )
    model = MultiOutputRegressor(base_model)
    model.fit(X_train, y_train)
    return model


def train_model_from_history(df_processed: pd.DataFrame, feature_cols: list):
    X = df_processed[feature_cols]
    y = df_processed[TARGET_COLS]
    model = train_new_model(X, y)
    return model
