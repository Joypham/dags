import os
from datetime import datetime, timedelta

from Config import *
from Email import Email
from vna_report_backdate.param import *

import json
import os
import pandas
import pysftp as sftp
import psycopg2

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)


def create_report_file(report_date, **kwargs):
    task_instance = kwargs['task_instance']

    try:
        report_date_object = datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)
        report_date = report_date_object.strftime('%Y-%m-%d')
        report_date_filename = report_date_object.strftime('%d%m%Y')
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
            value=json.dumps([
                {
                    "title": order_list_filename,
                    "path": f"{daily_report_path}/{order_list_filename}"
                },
                {
                    "title": code_usage_filename,
                    "path": f"{code_usage_filename}/{code_usage_filename}"
                },
                {
                    "title": order_list_filename,
                    "path": f"{cancelled_order_filename}/{cancelled_order_filename}"
                },
            ])
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


def send_email_internal(report_date, result, list_file):
    if result is False:
        Email.send_mail(INTERNAL_EMAIL, "Lỗi", "Có lỗi xảy ra")
    elif list_file is None:
        Email.send_mail(INTERNAL_EMAIL, "Lỗi", "Có lỗi xảy ra")
    else:
        Email.send_mail_with_attachment(
            receiver=[INTERNAL_EMAIL],
            subject="VNA Report Daily",
            content=f"File báo cáo gửi VNA ngày {report_date}",
            attachments=json.loads(list_file)
        )


def upload_to_vna_sftp(result, list_file):
    print("Upload to VNA here")
    # if result is True and list_file is not None:
    #     for host in VNA_HOST:
    #         server = sftp.Connection(host=host, username='urbox', password='Jul#020721Evoucher')
    #         with server.cd(VNA_FOLDER_PATH):  # chdir to public
    #             for file in json.loads(list_file):
    #                 server.put(file.get("path"))
    #         server.close()

# def main(report_date):
#     try:
#         report_date_object = datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)
#         report_date = report_date_object.strftime('%Y-%m-%d')
#         report_date_filename = report_date_object.strftime('%d%m%Y')
#         order_list_filename = f"VNA_Evoucher_Order_List_{report_date_filename}.xlsx"
#         code_usage_filename = f"VNA_Evoucher_Code_Used_{report_date_filename}.xlsx"
#         cancelled_order_filename = f"VNA_Evoucher_Order_List_Cancel_{report_date_filename}.xlsx"
#
#         daily_report_path = f"{STORAGE_DIR}/vna_report/{report_date}"
#
#         if not os.path.exists(daily_report_path):
#             os.mkdir(daily_report_path)
#
#         generate_excel(daily_report_path, order_list_filename, ORDER_LIST_QUERY.format(report_date=report_date))
#         generate_excel(daily_report_path, code_usage_filename, CODE_USAGE_QUERY.format(report_date=report_date))
#         generate_excel(daily_report_path, cancelled_order_filename, CANCELLED_ORDER_QUERY.format(report_date=report_date))
#
#         Email.send_mail_with_attachment(
#             receiver=["hanh.ph@urbox.vn"],
#             subject="VNA Report Daily",
#             content=f"File báo cáo gửi VNA ngày {report_date}",
#             attachments=[
#                 {
#                     "title": order_list_filename,
#                     "path": f"{daily_report_path}/{order_list_filename}"
#                 },
#                 {
#                     "title": code_usage_filename,
#                     "path": f"{daily_report_path}/{code_usage_filename}"
#                 },
#                 {
#                     "title": cancelled_order_filename,
#                     "path": f"{daily_report_path}/{cancelled_order_filename}"
#                 }
#             ]
#         )
#
#     except Exception as e:
#         print(e)


def generate_excel(path, filename, query):
    redshift_cursor = redshift_connection.cursor()
    redshift_cursor.execute(query)
    column = [item.name for item in redshift_cursor.description]
    data = pandas.DataFrame(redshift_cursor.fetchall(), columns=column)
    with open(f"{path}/{filename}", "wb") as f:
        with pandas.ExcelWriter(f, engine='xlsxwriter') as writer:
            data.to_excel(writer, index=False)
