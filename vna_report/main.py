from datetime import datetime, timedelta

from Config import *
from Email import Email
from Utility import Utility
from vna_report.param import *

import os
import pandas
import pysftp as sftp
import psycopg2

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)


def generate_report_date(report_date=None, **kwargs):
    if report_date is None:
        report_date_object = datetime.today() - timedelta(days=1)
    else:
        report_date_object = datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(
        key='report_date',
        value=report_date_object.strftime('%Y-%m-%d')
    )


def create_report_file(report_date, **kwargs):
    task_instance = kwargs['task_instance']
    report_date_object = datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)
    report_date_filename = report_date_object.strftime('%d%m%Y')

    try:
        order_list_filename = f"VNA_Evoucher_Order_List_{report_date_filename}.xlsx"
        code_usage_filename = f"VNA_Evoucher_Code_Used_{report_date_filename}.xlsx"
        cancelled_order_filename = f"VNA_Evoucher_Order_List_Cancel_{report_date_filename}.xlsx"

        daily_report_path = f"{STORAGE_DIR}/vna_report/{report_date}"

        if not os.path.exists(daily_report_path):
            os.mkdir(daily_report_path)

        generate_excel(daily_report_path, order_list_filename, ORDER_LIST_QUERY.format(report_date=report_date))
        generate_excel(daily_report_path, code_usage_filename, CODE_USAGE_QUERY.format(report_date=report_date))
        generate_excel(daily_report_path, cancelled_order_filename, CANCELLED_ORDER_QUERY.format(report_date=report_date))

        task_instance.xcom_push(
            key='result',
            value=True
        )
        task_instance.xcom_push(
            key='report_date',
            value=report_date
        )
        task_instance.xcom_push(
            key='list_file',
            value=[
                {
                    "title": order_list_filename,
                    "path": f"{daily_report_path}/{order_list_filename}"
                },
                {
                    "title": code_usage_filename,
                    "path": f"{daily_report_path}/{code_usage_filename}"
                },
                {
                    "title": cancelled_order_filename,
                    "path": f"{daily_report_path}/{cancelled_order_filename}"
                },
            ]
        )
    except Exception as e:
        print("DAG error: ")
        print(e)
        task_instance.xcom_push(
            key='result',
            value=False
        )
        task_instance.xcom_push(
            key='report_date',
            value=report_date
        )
        task_instance.xcom_push(
            key='list_file',
            value=None
        )


def send_email_internal(result, report_date, list_file):
    if result is False or list_file is None:
        Email.send_mail(
            receiver=INTERNAL_EMAIL,
            subject=f"[VNA Report Daily] Lỗi ngày {report_date}",
            content=f"Có lỗi không mong muốn xảy ra khi sinh báo cáo ngày {report_date}. Liên hệ nammk để xử lý!"
        )
    else:
        Email.send_mail_with_attachment(
            receiver=INTERNAL_EMAIL,
            subject=f"[VNA Report Daily] Báo cáo ngày {report_date}",
            content=f"Đã tạo, lưu trữ và gửi cho VNA các file báo cáo ngày {report_date}",
            attachments=list_file
        )


def upload_to_vna_sftp(result, list_file):
    print("upload vna")
    # if result is True and list_file is not None:
    #     cnopts = sftp.CnOpts()
    #     cnopts.hostkeys = None
    #     for host in VNA_HOST:
    #         server = sftp.Connection(host=host, username='urbox', password='Jul#020721Evoucher', cnopts=cnopts)
    #         with server.cd(VNA_FOLDER_PATH):  # chdir to public
    #             for file in list_file:
    #                 server.put(file.get("path"))
    #         server.close()


def end_dag(report_date):
    Utility.send_telegram_message(
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_IDS,
        f"DAGs gửi báo cáo VNA bổ sung ngày {report_date} đã chạy xong. Kiểm tra email để xem chi tiết."
    )


def generate_excel(path, filename, query):
    redshift_cursor = redshift_connection.cursor()
    redshift_cursor.execute(query)
    column = [item.name for item in redshift_cursor.description]
    data = pandas.DataFrame(redshift_cursor.fetchall(), columns=column)
    with open(f"{path}/{filename}", "wb") as f:
        with pandas.ExcelWriter(f, engine='xlsxwriter') as writer:
            data.to_excel(writer, index=False)
