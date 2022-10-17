from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from multi_source.code_tu_up_ma.main import code_tu_up_ma_to_redshift

default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'owner': 'hanhph',
    'email': ['hanh.ph@urbox.vn'],
    'start_date': datetime(2022, 6, 16, 0, 0),
    'retries': 3
}

dag = DAG(
    'code_tu_up_ma',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=32,
    schedule_interval="1 9 */1 * *",
    render_template_as_native_obj=True,
    description='đẩy dữ liệu code tu up ma từ gsheet lên redshift',
    tags=["code_tu_up_ma", "hybrid", "v1.0"],
)

code_tu_up_ma_to_redshift = PythonOperator(
    dag=dag,
    task_id="code_tu_up_ma_to_redshift",
    python_callable=code_tu_up_ma_to_redshift,

)
code_tu_up_ma_to_redshift
