from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from Sentry.main import generate_report_date, get_all_events_by_project

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
    'get_data_from_sentry',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval="1 0 */1 * *",
    render_template_as_native_obj=True,
    description='Lấy dữ liệu từ sentry',
    tags=["sentry", "hybrid", "v1"],
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

get_all_events_by_project = PythonOperator(
    dag=dag,
    task_id="get_all_events_by_project",
    python_callable=get_all_events_by_project,
    op_kwargs={
        "report_date": "{{ti.xcom_pull(task_ids='generate_report_date', key='report_date')}}",
    }
)
generate_report_date >> get_all_events_by_project
