from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ub_holistic.main import call_pcd
from ub_holistic.params import *

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
    'retries': 10
}

dag = DAG(
    'pcd_holistics',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    concurrency=32,
    schedule_interval="1 */2 */1 * *",
    render_template_as_native_obj=True,
    description='call các pcd trong schema ub_holistics',
    tags=["ub_holistics", "hybrid", "v1"],
    params={
        "pcd": "pcd_str"
    }
)

card_holistics = PythonOperator(
    dag=dag,
    task_id="card_holistics",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": card_holistics,
    }
)

cart_physical_holistic = PythonOperator(
    dag=dag,
    task_id="cart_physical_holistic",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": cart_physical_holistic,
    }
)

holistics_cart_detail_loyalty = PythonOperator(
    dag=dag,
    task_id="holistics_cart_detail_loyalty",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": holistics_cart_detail_loyalty,
    }
)

holistics_cart_detail = PythonOperator(
    dag=dag,
    task_id="holistics_cart_detail",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": holistics_cart_detail,
    }
)

holistic_redemption = PythonOperator(
    dag=dag,
    task_id="holistic_redemption",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": holistic_redemption,
    }
)

holistic_redemption_last_3m = PythonOperator(
    dag=dag,
    task_id="holistic_redemption_last_3m",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": holistic_redemption_last_3m,
    }
)

holistics_gift_code_created = PythonOperator(
    dag=dag,
    task_id="holistics_gift_code_created",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": holistics_gift_code_created,
    }
)
holistics_redemption_last_using_3m = PythonOperator(
    dag=dag,
    task_id="holistics_redemption_last_using_3m",
    python_callable=call_pcd,
    op_kwargs={
        "pcd": holistics_redemption_last_using_3m,
    }
)

card_holistics >> cart_physical_holistic
card_holistics >> holistics_cart_detail
card_holistics >> holistics_cart_detail_loyalty
# card_holistics >> holistic_redemption
card_holistics >> holistic_redemption_last_3m >> holistics_redemption_last_using_3m
holistics_gift_code_created

