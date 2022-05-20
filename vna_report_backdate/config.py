from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from vna_report_backdate.main import create_report_file, send_email_internal, upload_to_vna_sftp


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
    tags=["vna", "manual", "v0.5"],
    params={
        "report_date": "%Y-%m-%d"
    }
)

create_report_file = PythonOperator(
    dag=dag,
    task_id="create_report_file",
    python_callable=create_report_file,
    op_kwargs={
        'report_date': "{{dag_run.conf['report_date']}}"
    }

)
send_email_internal = PythonOperator(
    dag=dag,
    task_id="send_email_internal",
    python_callable=send_email_internal,
    render_template_as_native_obj=True,
    op_kwargs={
        "result": "{{ti.xcom_pull(task_ids='create_report_file', key='result')}}",
        "report_date": "{{ti.xcom_pull(task_ids='create_report_file', key='report_date')}}",
        "list_file": "{{ti.xcom_pull(task_ids='create_report_file', key='list_file')}}",
    }
)
upload_to_vna_sftp = PythonOperator(
    dag=dag,
    task_id="upload_to_vna_sftp",
    python_callable=upload_to_vna_sftp,
    render_template_as_native_obj=True,
    op_kwargs={
        "result": "{{ti.xcom_pull(task_ids='create_report_file', key='result')}}",
        "list_file": "{{ti.xcom_pull(task_ids='create_report_file', key='list_file')}}",
    }
)

create_report_file >> send_email_internal
create_report_file >> upload_to_vna_sftp

# export_report_and_upload = PythonOperator(
#     task_id='export_report_and_upload',
#     dag=dag,
#     python_callable=main,
#     op_kwargs={
#         'report_date': "{{dag_run.conf['report_date']}}"
#     }
# )
# export_report_and_upload  # noqa
