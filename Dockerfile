FROM apache/airflow:3.1.7

RUN pip install --no-cache-dir --upgrade \
    urllib3 \
    requests \
    openmeteo-requests \
    requests-cache \
    retry-requests \
    pyyaml \
    pandas \
    psycopg2-binary \
    scikit-learn \
    xgboost \
    joblib