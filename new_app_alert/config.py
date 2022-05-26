from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from new_app_alert.main import main

default_args = {
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),

    'owner': 'nammk',
    'email': ['nam.mk@urbox.vn'],
    'start_date': datetime(2022, 5, 23, 0, 0),
    'retries': 3
}

dag = DAG(
    'new_app_alert',
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval=None,
    render_template_as_native_obj=True,
    description='DAG thông báo khi có app mới hoặc app được cập nhật',
    tags=["brand", "auto", "v0.1"]
)

get_exist_app = PythonOperator(
    dag=dag,
    task_id="get_exist_app",
    python_callable=main
)

get_new_app = PythonOperator(
    dag=dag,
    task_id="get_new_app",
    python_callable=main
)

get_updated_app = PythonOperator(
    dag=dag,
    task_id="get_updated_app",
    python_callable=main
)

send_alert = PythonOperator(
    dag=dag,
    task_id="send_alert",
    python_callable=main
)

get_exist_app >> get_new_app >> send_alert
get_exist_app >> get_updated_app >> send_alert
