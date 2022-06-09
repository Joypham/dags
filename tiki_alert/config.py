from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from tiki_alert.main import push_notification, generate_report_datetime

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
    'tiki_alert',
    # catchup=False,
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval="1 */1 * * *",
    render_template_as_native_obj=True,
    description='Gửi email khi lượng đổi hoặc lượt đổi vượt mức bình thường',
    tags=["tiki", "hybrid", "v1"],
    params={
            "report_datetime": "%Y-%m-%d %H:%M:%S"
        }
)

generate_report_datetime = PythonOperator(
    dag=dag,
    task_id="generate_report_datetime",
    python_callable=generate_report_datetime,
    op_kwargs={
            'report_datetime': "{{params.report_datetime}}"
        }
)
push_notification = PythonOperator(
    dag=dag,
    task_id="push_notification",
    python_callable=push_notification,
    op_kwargs={
        "report_datetime": "{{ti.xcom_pull(task_ids='generate_report_datetime', key='report_datetime')}}",
    }
)
generate_report_datetime >> push_notification
