from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
    'brand_threshold_alert',
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval=None,
    render_template_as_native_obj=True,
    description='DAG cảnh báo khi brand đạt ngưỡng doanh thu',
    tags=["brand", "auto", "v0.1"]
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
