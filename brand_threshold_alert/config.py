from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from brand_threshold_alert.main import main

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),

    'owner': 'nammk',
    'email': ['nam.mk@urbox.vn'],
    'start_date': datetime(2022, 5, 25, 0, 0),
    'retries': 3
}

dag = DAG(
    'brand_threshold_alert',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=32,
    schedule_interval="0 0 * * *",
    render_template_as_native_obj=True,
    description='DAG cảnh báo khi brand đạt ngưỡng doanh thu',
    tags=["brand", "auto", "v1.0"]
)

brand_threshold_alert = PythonOperator(
    dag=dag,
    task_id="brand_threshold_alert",
    python_callable=main
)

brand_threshold_alert   # noqa
