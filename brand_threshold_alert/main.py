# from oauth2client.service_account import ServiceAccountCredentials
from brand_threshold_alert.param import *

from datetime import datetime
from Config import *
from Email import Email
from Utility import Utility

import gspread
import psycopg2
import psycopg2.extras

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)

google_cloud = gspread.service_account(filename=GOOGLE_PRIVATE_KEY)
google_spread = google_cloud.open("Cảnh báo doanh thu đạt ngưỡng")


def main():
    current = Utility.current_timestamp()
    list_brand = get_brand_config()
    list_payment = get_payment()
    list_unrecorded_revenue = get_unrecorded_revenue()
    list_mail = get_list_mail()
    list_log = get_log()
    for id, threshold in list_brand.items():
        print(f"Kiểm tra dữ liệu brand: {id}")
        revenue = get_revenue_by_brand_id(id)
        unrecorded_revenue = list_unrecorded_revenue.get(f"{id}") or 0
        paid_amount = list_payment.get(f"{id}") or None

        log = list_log.get(f"{id}") or None
        if log is not None:
            latest_level = log.get("level")
            latest_log = log.get("latest_log")
        else:
            latest_level = 0
            latest_log = 0

        remaining_revenue = revenue.get("revenue") + unrecorded_revenue - paid_amount
        print(f"Thông tin brand: {revenue.get('title')}")
        print(f"Tổng doanh thu đã ghi nhận trên hệ thống là {revenue.get('revenue'):,}")
        print(f"Tổng doanh thu chưa ghi nhận trên hệ thống là {unrecorded_revenue:,}")
        print(f"Tổng số tiền đã thanh toán là {paid_amount:,}")
        print(f"Số tiền chưa thanh toán là {remaining_revenue:,}")
        if remaining_revenue <= 0:
            print("Đã thanh toán tất cả")
            continue

        warning_level = check_threshold(threshold, remaining_revenue)
        if warning_level is False:
            print("Số tiền chưa thanh toán không chạm mốc cảnh báo.")
            continue

        diff_latest_log_hours = (current - latest_log) // 3600
        if latest_level == warning_level and diff_latest_log_hours < 24:
            print("Brand này đã được cảnh báo trong 24h vừa qua")
            continue
        print(f"""
            Brand {revenue.get('title')} đã chạm tới mốc {warning_level} 
            với doanh thu chưa được thanh toán là {remaining_revenue:,}
        """)
        if warning_level == 1:
            list_receiver = list_mail.get(f"{id}").get("low")
        else:
            list_receiver = list_mail.get(f"{id}").get("high")
        Email.send_mail(
            receiver=list_receiver,
            subject=SUBJECT.format(brand_name=revenue.get('title')),
            content=CONTENT.get(f"{warning_level}").format(brand_name=revenue.get('title'), revenue=remaining_revenue)
        )
        create_log(id, warning_level)


def get_brand_config():
    config_sheet = google_spread.worksheet("config")
    config_data = config_sheet.get_all_records()
    list_brand = {}
    for config in config_data:
        key = f"{config.get('brand_id')}"
        if key not in list_brand:
            list_brand.update({key: {}})
        list_brand.get(key).update({
            f"{config.get('threshold_level')}": Utility.to_int(config.get('value'))
        })
    return list_brand


def get_payment():
    payment_sheet = google_spread.worksheet("payment")
    payment_data = payment_sheet.get_all_records()
    list_payment = {}
    for payment in payment_data:
        key = f"{payment.get('brand_id')}"
        if key not in list_payment:
            list_payment.update({key: 0})
        payment_amount = list_payment.get(key) + payment.get("payment_amount")
        list_payment.update({key: payment_amount})
    return list_payment


def get_unrecorded_revenue():
    unrecorded_code_sheet = google_spread.worksheet("unrecorded_code")
    unrecorded_code_data = unrecorded_code_sheet.get_all_records()
    list_unrecorded_revenue = {}
    for code in unrecorded_code_data:
        key = f"{code.get('brand_id')}"
        if key not in list_unrecorded_revenue:
            list_unrecorded_revenue.update({
                key: 0
            })
        list_unrecorded_revenue.update({
            key: list_unrecorded_revenue.get(key) + code.get("value")
        })
    return list_unrecorded_revenue


def get_list_mail():
    mail_sheet = google_spread.worksheet("mail_list")
    mail_data = mail_sheet.get_all_records()
    list_mail = {}
    for mail in mail_data:
        key = f"{mail.get('brand_id')}"
        if key not in list_mail:
            list_mail.update({
                key: {
                    "low": [],
                    "high": []
                }
            })
        list_mail.get(key).get("high").append(mail.get("mail"))
        if mail.get('type') == 1:
            list_mail.get(key).get("low").append(mail.get("mail"))
    return list_mail


def get_log():
    log_sheet = google_spread.worksheet("send_log")
    log_data = log_sheet.get_all_records()
    list_log = {}
    for log in log_data:
        key = f"{log.get('brand_id')}"
        if key not in list_log:
            list_log.update({
                key: {
                    "level": 0,
                    "latest_log": 0
                }
            })
        latest_log = Utility.date_string_to_timestamp(log.get("log_time"))
        if list_log.get(key).get("latest_log") >= latest_log:
            continue
        list_log.get(key).update({
            "level": log.get("threshold_level"),
            "latest_log": latest_log
        })
    return list_log


def create_log(brand_id, threshold_level):
    log_sheet = google_spread.worksheet("send_log")
    log_sheet.append_row([brand_id, threshold_level, str(datetime.today())])


def get_revenue_by_brand_id(brand_id):
    redshift_cursor = redshift_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    redshift_cursor.execute(f"""
        SELECT b.title, SUM(cd.money) AS revenue
        FROM urbox.cart_detail cd
            LEFT JOIN urbox.cart c ON cd.cart_id = c.id
            LEFT JOIN (
                SELECT *, row_number() over(PARTITION BY cart_detail_id ORDER BY id DESC) rn_cd_id FROM urbox.gift_code
            ) gc ON gc.cart_detail_id = cd.id AND gc.rn_cd_id = 1 AND gc.status = 1
            LEFT JOIN urbox.brand b ON b.id = cd.brand_id
        WHERE
            cd.status = 2
            AND cd.pay_status = 2
            AND c.delivery <> 4
            AND gc.used > 0
            AND gc.used IS NOT NULL
            AND cd.brand_id = {brand_id}
        GROUP BY b.title
    """)
    result = redshift_cursor.fetchone()
    redshift_cursor.close()
    return result


def check_threshold(threshold, revenue):
    for level, value in threshold.items():
        if revenue >= value:
            return level
    return False
