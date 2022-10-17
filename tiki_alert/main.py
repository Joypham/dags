from tiki_alert.param import *
from Config import *
import re
import pandas as pd
from Email import Email
from sqlalchemy import create_engine

URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
redshift_connection = create_engine(URI)
pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)


def generate_report_datetime(report_datetime=None, **kwargs):
    if not report_datetime or report_datetime == "%Y-%m-%d %H:%M:%S":
        report_date_object = datetime.now() + timedelta(hours=7)
    else:
        report_date_object = datetime.strptime(report_datetime, '%Y-%m-%d %H:%M:%S')

    task_instance = kwargs['task_instance']
    task_instance.xcom_push(
        key='report_datetime',
        value=report_date_object.strftime("%Y-%m-%d %H:%M:%S")
    )


def thredsold_by_total_redemption(report_datetime: datetime = datetime.now(), **kwargs):
    dict_hour = {"class_1": ClassificationHour.class_1,
                 "class_2": ClassificationHour.class_2,
                 "class_3": ClassificationHour.class_3,
                 "class_4": ClassificationHour.class_4,
                 "class_5": ClassificationHour.class_5,
                 "class_6": ClassificationHour.class_6,
                 "class_7": ClassificationHour.class_7,
                 "class_8": ClassificationHour.class_8,
                 "class_9": ClassificationHour.class_9}
    start_date = report_datetime.date() - timedelta(days=1)
    end_7_date = report_datetime.date() - timedelta(days=7)
    list_hour = []
    one_hours_before = report_datetime - timedelta(hours=1)
    one_hours_before = one_hours_before.hour
    for key, value in dict_hour.items():
        if one_hours_before in value:
            list_hour = value
        else:
            pass
    # trung bình lượt/ lượng đổi trong 1 giờ của 1 tuần gần nhất theo 9 nhóm ở trên.
    # Note: chỉ tính những giờ có phát sinh giao dịch, loại bỏ giao dịch của các bên chuyên mua code (grab reward...): card_id > 0
    list_hour = tuple(list_hour)
    list_hour = re.sub(r',(?=\))', '', str(list_hour))
    avg_so_luot_doi = pd.read_sql_query(
        sql=AVG_REDEMPTION_RECORDS.format(start_date=start_date, end_7_date=end_7_date, list_hour=list_hour),
        con=redshift_connection
    )
    avg_so_luot_doi = avg_so_luot_doi.values.tolist()[0][0]

    avg_luong_doi = pd.read_sql_query(
        sql=AVG_REDEMPTION.format(start_date=start_date, end_7_date=end_7_date, list_hour=list_hour),
        con=redshift_connection
    )
    avg_luong_doi = avg_luong_doi.values.tolist()[0][0]
    return {'avg_so_luot_doi': avg_so_luot_doi, 'avg_luong_doi': avg_luong_doi}


def get_total_redemption_at_current_hour(report_datetime: datetime = datetime.now(), **kwargs):
    thredsold = thredsold_by_total_redemption(report_datetime=report_datetime)

    # Thuc te
    date_now = report_datetime.date()
    one_hours_before = report_datetime - timedelta(hours=1)
    one_hours_from_now = one_hours_before.hour
    redemption_at_current_hour = pd.read_sql_query(
        sql=REDEMPTION_AT_CURRENT_HOUR.format(date_now=date_now, one_hours_from_now=one_hours_from_now),
        con=redshift_connection)
    so_luot_doi_at_current_hour = redemption_at_current_hour['so_luot_doi'].tolist()[0]
    luong_doi_at_current_hour = redemption_at_current_hour['luong_doi'].tolist()[0]

    # So sanh tinh ty trong
    avg_so_luot_doi = thredsold['avg_so_luot_doi']
    avg_luong_doi = thredsold['avg_luong_doi']
    rate_luot_doi = int((so_luot_doi_at_current_hour - avg_so_luot_doi) / avg_so_luot_doi * 100)
    rate_luong_doi = int((luong_doi_at_current_hour - avg_luong_doi) / avg_luong_doi * 100)
    diff_luot_doi = so_luot_doi_at_current_hour - avg_so_luot_doi
    diff_luong_doi = luong_doi_at_current_hour - avg_luong_doi
    return {"avg_so_luot_doi": avg_so_luot_doi,
            "so_luot_doi_at_current_hour": so_luot_doi_at_current_hour,
            "avg_luong_doi": avg_luong_doi,
            "luong_doi_at_current_hour": luong_doi_at_current_hour,
            "rate_luot_doi": rate_luot_doi,
            "rate_luong_doi": rate_luong_doi,
            "diff_luot_doi": diff_luot_doi,
            "diff_luong_doi": diff_luong_doi,
            "type": "Tổng danh mục"}


def get_range_alert(report_datetime=None, **kwargs):
    dict_range = {"level_negative": AlertLevel.level_negative,
                  "level_0": AlertLevel.level_0,
                  "level_1": AlertLevel.level_1,
                  "level_2": AlertLevel.level_2,
                  "level_3": AlertLevel.level_3,
                  "level_4": AlertLevel.level_4,
                  "level_5": AlertLevel.level_5
                  }
    one_hours_before = report_datetime - timedelta(hours=1)
    time_range = f"{report_datetime.date()}: {one_hours_before.hour}h - {report_datetime.hour}h"

    # Get level:
    k1 = get_total_redemption_at_current_hour(report_datetime=report_datetime)
    k1['time_range'] = time_range
    if abs(k1['rate_luot_doi']) > abs(k1['rate_luong_doi']):
        rate = k1['rate_luot_doi']
    else:
        rate = k1['rate_luong_doi']
    for key, value in dict_range.items():
        if rate in range(value[0], value[1]):
            k1['level'] = key
        else:
            pass
    return k1


def push_notification(report_datetime: datetime = None, **kwargs):
    # report_datetime =kwargs['dag_run'].conf['report_datetime']
    # execution_date = (data_interval_start + timedelta(hours=7))
    # report_datetime = datetime.strptime(report_datetime, "%Y-%m-%d %H:%M:%S") if report_datetime else execution_date

    report_datetime = datetime.strptime(report_datetime, '%Y-%m-%d %H:%M:%S')

    k2 = get_range_alert(report_datetime=report_datetime)
    df = pd.DataFrame([k2])

    df.to_sql("tiki_monitoring", con=redshift_connection, if_exists='append', index=False,
              schema='ub_rawdata')
    luong_doi_at_current_hour = k2.get('luong_doi_at_current_hour')
    level = k2.get("level")
    content = ALERT_EMAIL_CONTENT.format(
        time_range=k2.get('time_range'),
        type=k2.get('type'),
        level=level,
        rate_luong_doi=k2.get('rate_luong_doi'),
        rate_luot_doi=k2.get('rate_luot_doi'),
        avg_so_luot_doi=k2.get('avg_so_luot_doi'),
        so_luot_doi_at_current_hour=k2.get('so_luot_doi_at_current_hour'),
        avg_luong_doi="{:,}".format(k2.get('avg_luong_doi')),
        luong_doi_at_current_hour="{:,}".format(k2.get('luong_doi_at_current_hour'))
    )
    if level not in ('level_negative', 'level_0') and luong_doi_at_current_hour > 15000000:
        Email.send_mail(
            receiver=TIKI_ALERT_EMAIL,
            subject=f"[Tiki warning] Cảnh báo lượng đổi tiki vượt ngưỡng {k2.get('time_range')}",
            content=content
        )


if __name__ == "__main__":
    # push_notification()
    thredsold_by_total_redemption()
