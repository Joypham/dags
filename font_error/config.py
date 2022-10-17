from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from font_error.main import mobile_rate_tv

# Crontab scheduler: https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html

default_args = {
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'owner': 'hanhph',
    'email': ['hanh.ph@urbox.vn'],
    # edit start date here
    'start_date': datetime(2022, 6, 3, 0, 0),
    'retries': 3
}

dag = DAG(
    'mobile_rate_tv',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval="1 0 */1 * *",
    render_template_as_native_obj=True,
    description='reformat lại các data bị lỗi font trong redshift',
    tags=["font_error", "hybrid", "v1"]
)
mobile_rate_tv = PythonOperator(
    dag=dag,
    task_id="get_new_rating",
    python_callable=mobile_rate_tv,
)

mobile_rate_tv
