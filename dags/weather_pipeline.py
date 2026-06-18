from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from airflow.sdk import dag, task
import pandas as pd
from utils import insert_records as ir
from utils import db_queries as dq
from utils import api_calls, data_processing, model_utils

default_args = {
    "owner": "Maksat",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="Weather_Pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 10),
    schedule="0 0 * * *",
    catchup=False,
)
def weather_pipeline():

    @task
    def extract_yesterday_data() -> list:
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        df = api_calls.get_data(start_date=yesterday, end_date=yesterday)
        df["date"] = df["date"].astype(str)
        return df.to_dict("records")

    @task
    def insert_raw_data(raw_records: list):
        conn = ir.db_connect()
        ir.create_all_tables(conn)
        raw_df = pd.DataFrame(raw_records)
        raw_df["date"] = pd.to_datetime(raw_df["date"])
        ir.insert_raw_weather(conn, "city_center", raw_df)
        conn.close()

    @task
    def evaluate_then_retrain():
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        actuals = dq.get_actuals_for_date(yesterday)
        predictions = dq.get_predictions_for_date(yesterday)
        if actuals.empty or predictions.empty:
            print("No predictions to evaluate yet")
            return
        metrics_df = model_utils.evaluate_predictions(actuals, predictions)
        conn = ir.db_connect()
        ir.insert_model_metrics(conn, model_utils.MODEL_PATH, metrics_df, retrained=False)
        if model_utils.should_retrain(metrics_df):
            print("Performance below threshold, retraining model...")
            df = dq.get_latest_processed_data(lookback_hours=168 * 4)
            if not df.empty:
                df_processed = data_processing.engineer_features(df)
                feature_cols = data_processing.get_feature_columns(df_processed)
                model = model_utils.train_model_from_history(df_processed, feature_cols)
                model_utils.save_model(model)
                ir.insert_model_metrics(conn, model_utils.MODEL_PATH, metrics_df, retrained=True)
                print("Model retrained and saved")
        conn.close()

    @task
    def predict_next_24h():
        df = dq.get_latest_processed_data(lookback_hours=168)
        if df.empty:
            print("No data for prediction")
            return
        df_features = data_processing.engineer_features_for_prediction(df)
        feature_cols = data_processing.get_feature_columns(df_features)
        if len(df_features) < 24:
            print("Not enough data to engineer features")
            return
        model = model_utils.load_model()
        predictions = model_utils.predict_next_24h(df_features, feature_cols, model)
        conn = ir.db_connect()
        ir.insert_predictions(conn, predictions)
        conn.close()
        print(f"Inserted {len(predictions)} predictions for next 24 hours")

    raw = extract_yesterday_data()
    insert_raw_data(raw) >> evaluate_then_retrain() >> predict_next_24h()


dag_instance = weather_pipeline()
