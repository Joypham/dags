from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from multi_source.premium_service.main import premium_service_to_redshift, brand_ps_to_redshift, cs_premium_code_null_redshift

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
    'premium_service',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=32,
    schedule_interval="1 0 */1 * *",
    render_template_as_native_obj=True,
    description='đẩy dữ liệu premium_service từ gsheet lên redshift',
    tags=["premium_service", "hybrid", "v1.0"],
)

premium_service_to_redshift = PythonOperator(
    dag=dag,
    task_id="premium_service_to_redshift",
    python_callable=premium_service_to_redshift,

)

brand_ps_to_redshift = PythonOperator(
    dag=dag,
    task_id="brand_ps_to_redshift",
    python_callable=brand_ps_to_redshift,

)

cs_premium_code_null_redshift = PythonOperator(
    dag=dag,
    task_id="cs_premium_code_null_redshift",
    python_callable=cs_premium_code_null_redshift,

)
brand_ps_to_redshift >> premium_service_to_redshift >> cs_premium_code_null_redshift
