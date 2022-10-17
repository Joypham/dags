from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from multi_source.PO.main import call_pcd, sub_po_to_redshift, po_to_redshift

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
    'orther_source_PO',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=32,
    schedule_interval="1 9 */1 * *",
    render_template_as_native_obj=True,
    description='đẩy dữ liệu PO từ gsheet lên redshift',
    tags=["po", "hybrid", "v1.0"],
)

sub_po_to_redshift = PythonOperator(
    dag=dag,
    task_id="sub_po_to_redshift",
    python_callable=sub_po_to_redshift,

)
po_to_redshift = PythonOperator(
    dag=dag,
    task_id="po_to_redshift",
    python_callable=po_to_redshift,

)
call_pcd = PythonOperator(
    dag=dag,
    task_id="call_pcd",
    python_callable=call_pcd,

)
sub_po_to_redshift >> call_pcd
po_to_redshift >> call_pcd
