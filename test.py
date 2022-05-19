from Packages import *

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
    'hello_world',
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval=None
)


def main():
    print("Hello World")


hello_world = PythonOperator(
    task_id='hello_world',
    dag=dag,
    python_callable=main
)

hello_world  # noqa
