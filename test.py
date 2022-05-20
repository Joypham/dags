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
    'test_xcom',
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval=None
)


def method1(**kwargs):
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='name', value="Nam")


def method2(name):
    print(f"Hello {name}")


task_1 = PythonOperator(
    task_id='task_1',
    dag=dag,
    python_callable=method1
)
task_2 = PythonOperator(
    task_id='task_2',
    dag=dag,
    python_callable=method2,
    op_kwargs={
        "name": "{{ti.xcom_pull(task_ids='task_1', key='name')}}"
    }
)
task_1 >> task_2  # noqa
