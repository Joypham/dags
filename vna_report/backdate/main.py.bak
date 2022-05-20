from datetime import datetime, timedelta


def generate_report_date(report_date, **kwargs):
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(
        key='report_date_object',
        value=datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)
    )
