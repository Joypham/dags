from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from multi_source.cs_data_trainning.main import cs_data_tranning_to_redshift

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
    'cs_data_tranning',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=32,
    schedule_interval="1 9 */1 * *",
    render_template_as_native_obj=True,
    description='đẩy dữ liệu cs_data_tranning từ gsheet lên redshift',
    tags=["cs_data_tranning", "hybrid", "v1.0"],
)

cs_data_tranning_to_redshift = PythonOperator(
    dag=dag,
    task_id="cs_data_tranning_to_redshift",
    python_callable=cs_data_tranning_to_redshift,

)
cs_data_tranning_to_redshift
