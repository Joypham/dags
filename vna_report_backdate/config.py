from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from vna_report_backdate.main import main


default_args = {
    'depends_on_past': False,
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),

    'owner': 'nammk',
    'email': ['nam.mk@urbox.vn'],
    'start_date': datetime(2022, 5, 18, 0, 0),
    'retries': 3
}

dag = DAG(
    'vna_report_upload_backdate',
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval=None,
    description='DAG xuất báo cáo cho VNA theo ngày nhất định. Chỉ chạy bằng Trigger DAG w/ config',
    tags=["vna", "manual", "v0.2.1"],
    # params={
    #     "report_date": "2022-5-16"
    # }
)

export_report_and_upload = PythonOperator(
    task_id='export_report_and_upload',
    dag=dag,
    python_callable=main,
    op_kwargs={
        # 'report_date': "{{dag_run.conf['report_date']}}"
        'report_date': "2022-5-16"
    }
)
export_report_and_upload  # noqa
