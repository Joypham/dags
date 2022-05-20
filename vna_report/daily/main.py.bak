from datetime import datetime, timedelta


def generate_report_date(**kwargs):
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(
        key='report_date_object',
        value=datetime.today() - timedelta(days=1)
    )
