from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fake_so.app_217 import main

# default_args = {
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retry_delay': timedelta(minutes=1),
#     'owner': 'hanhph',
#     'email': ['hanh.ph@urbox.vn'],
#     'start_date': datetime(2022, 6, 16, 0, 0),
#     'retries': 3
# }
#
# dag = DAG(
#     'aia_report',
#     default_args=default_args,
#     max_active_runs=1,
#     catchup=False,
#     concurrency=32,
#     schedule_interval="1 */2 */1 * *",
#     render_template_as_native_obj=True,
#     description='báo cáo AIA',
#     tags=["Báo cáo AIA", "hybrid", "v1.0"],
# )
#
# aia_chi_tiet_the_phat_hanh = PythonOperator(
#     dag=dag,
#     task_id="aia_chi_tiet_the_phat_hanh",
#     python_callable=main_aia_chi_tiet_the_phat_hanh,
#
# )
# aia_chi_tiet_doi_qua = PythonOperator(
#     dag=dag,
#     task_id="aia_chi_tiet_doi_qua",
#     python_callable=main_aia_chi_tiet_doi_qua,
#
# )
# aia_danh_sach_po = PythonOperator(
#     dag=dag,
#     task_id="aia_danh_sach_po",
#     python_callable=main_aia_danh_sach_po,
#
# )
# aia_chi_tiet_the_phat_hanh >> aia_danh_sach_po
# aia_chi_tiet_doi_qua >> aia_danh_sach_po
