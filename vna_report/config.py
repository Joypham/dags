from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from vna_report.main import generate_report_date, create_report_file, send_email_internal, upload_to_vna_sftp, end_dag


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
    'vna_report_upload',
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    concurrency=32,
    schedule_interval="0 0 * * *",
    render_template_as_native_obj=True,
    description='DAG xuất báo cáo cho VNA. Nếu muốn xuất lại theo ngày nhất định thì chạy bằng Trigger DAG w/ config',
    tags=["vna", "hybrid", "v1.0"],
    params={
        "report_date": "%Y-%m-%d"
    }
)

generate_report_date = PythonOperator(
    dag=dag,
    task_id="generate_report_date",
    python_callable=generate_report_date,
    op_kwargs={
        'report_date': "{{params.report_date}}"
    }
)
create_report_file = PythonOperator(
    dag=dag,
    task_id="create_report_file",
    python_callable=create_report_file,
    op_kwargs={
        "report_date": "{{ti.xcom_pull(task_ids='generate_report_date', key='report_date')}}"
    }

)
send_email_internal = PythonOperator(
    dag=dag,
    task_id="send_email_internal",
    python_callable=send_email_internal,
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
    op_kwargs={
        "result": "{{ti.xcom_pull(task_ids='create_report_file', key='result')}}",
        "list_file": "{{ti.xcom_pull(task_ids='create_report_file', key='list_file')}}",
    }
)
end_dag = PythonOperator(
    dag=dag,
    task_id="end_dag",
    python_callable=end_dag,
    op_kwargs={
        "report_date": "{{ti.xcom_pull(task_ids='create_report_file', key='report_date')}}",
    }
)

generate_report_date >> create_report_file >> send_email_internal >> end_dag
generate_report_date >> create_report_file >> upload_to_vna_sftp >> end_dag
