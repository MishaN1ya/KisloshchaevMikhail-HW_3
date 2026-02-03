from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

INPUT_PATH = "/opt/airflow/data/IOT-temp.csv"
OUTPUT_DIR = "/opt/airflow/data"

def process_weather_data(**context):
    raw_df = pd.read_csv(INPUT_PATH, encoding='utf-8')

    filtered_df = raw_df[raw_df['out/in'] == 'In'].copy()

    filtered_df['parsed_date'] = pd.to_datetime(
        filtered_df['noted_date'],
        format='%m-%d-%Y %H:%M',
        errors='coerce'
    )
    dated_df = filtered_df.dropna(subset=['parsed_date']).copy()
    dated_df['noted_date'] = dated_df['parsed_date'].dt.date
    dated_df = dated_df.drop(columns=['parsed_date'])

    lower = dated_df['temp'].quantile(0.05)
    upper = dated_df['temp'].quantile(0.95)
    cleaned_df = dated_df[
        (dated_df['temp'] >= lower) & (dated_df['temp'] <= upper)
    ].copy()

    cleaned_df.to_csv(f"{OUTPUT_DIR}/result.csv", index=False)

    daily = cleaned_df.groupby('noted_date')['temp'].mean().reset_index()
    top_hot = daily.nlargest(5, 'temp')
    top_cold = daily.nsmallest(5, 'temp')
    top_hot.to_csv(f"{OUTPUT_DIR}/top_5_hottest_days.csv", index=False)
    top_cold.to_csv(f"{OUTPUT_DIR}/top_5_coldest_days.csv", index=False)

with DAG(
    dag_id="weather_analysis",
    start_date=datetime(2026, 2, 1),
    schedule_interval="@once",
    catchup=False
) as dag:

    PythonOperator(
        task_id="process_weather_data",
        python_callable=process_weather_data
    )