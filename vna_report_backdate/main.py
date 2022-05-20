import os
from datetime import datetime, timedelta

from Config import *
from Email import Email
from vna_report_backdate.param import *

import os
import pandas
import pysftp as sftp
import psycopg2

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)


def main(report_date):
    try:
        pass
        # report_date_object = datetime.strptime(report_date, '%Y-%m-%d') - timedelta(days=1)
        # report_date = report_date_object.strftime('%Y-%m-%d')
        # report_date_filename = report_date_object.strftime('%d%m%Y')
        # order_list_filename = f"VNA_Evoucher_Order_List_{report_date_filename}.xlsx"
        # code_usage_filename = f"VNA_Evoucher_Code_Used_{report_date_filename}.xlsx"
        # cancelled_order_filename = f"VNA_Evoucher_Order_List_Cancel_{report_date_filename}.xlsx"
        #
        # daily_report_path = f"{STORAGE_DIR}/vna_report/{report_date}"
        #
        # if not os.path.exists(daily_report_path):
        #     os.mkdir(daily_report_path)
        #
        # generate_excel(daily_report_path, order_list_filename, ORDER_LIST_QUERY.format(report_date=report_date))
        # generate_excel(daily_report_path, code_usage_filename, CODE_USAGE_QUERY.format(report_date=report_date))
        # generate_excel(daily_report_path, cancelled_order_filename, CANCELLED_ORDER_QUERY.format(report_date=report_date))

        # Email.send_mail_with_attachment(
        #     receiver=["hanh.ph@urbox.vn"],
        #     subject="VNA Report Daily",
        #     content=f"File báo cáo gửi VNA ngày {report_date}",
        #     attachments=[
        #         {
        #             "title": order_list_filename,
        #             "path": f"{daily_report_path}/{order_list_filename}"
        #         },
        #         {
        #             "title": code_usage_filename,
        #             "path": f"{daily_report_path}/{code_usage_filename}"
        #         },
        #         {
        #             "title": cancelled_order_filename,
        #             "path": f"{daily_report_path}/{cancelled_order_filename}"
        #         }
        #     ]
        # )

        # s = sftp.Connection(host=host, username='urbox', password='Jul#020721Evoucher')
        # remote_path = "/urbox_data/"
        #
        # file = f"VNA_Evoucher_Orderlist_Cancel/VNA_Evoucher_Orderlist_Cancel_{report_date}{ext}
        # with s.open(remote_path + file, "w") as f:
        #     f.write(order_list_cancel.to_csv(index=False))
        # s.close()
    except Exception as e:
        print(e)


def generate_excel(path, filename, query):
    redshift_cursor = redshift_connection.cursor()
    redshift_cursor.execute(query)
    column = [item.name for item in redshift_cursor.description]
    data = pandas.DataFrame(redshift_cursor.fetchall(), columns=column)
    with open(f"{path}/{filename}", "wb") as f:
        with pandas.ExcelWriter(f, engine='xlsxwriter') as writer:
            data.to_excel(writer, index=False)
