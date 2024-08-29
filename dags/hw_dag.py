import datetime as dt
import os
import sys
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
path = "/opt/airflow/plugins"
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict
# <YOUR_IMPORTS>

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 8, 13),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
    )
    predict = PythonOperator(
        task_id='predict',
        python_callable=predict
    )
    pipeline >> predict

