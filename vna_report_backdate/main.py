import os
from datetime import datetime, timedelta

from Config import *
from Email import Email
from vna_report_backdate.param import *

import os
import pandas
import psycopg2

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)


def main(report_date):
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

        redshift_cursor = redshift_connection.cursor()
        redshift_cursor.execute(ORDER_LIST_QUERY.format(start_date=report_date, end_date=report_date))
        column = [item.name for item in redshift_cursor.description]
        data = pandas.DataFrame(redshift_cursor.fetchall(), columns=column)
        with open(f"{daily_report_path}/{order_list_filename}", "wb") as f:
            with pandas.ExcelWriter(f, engine='xlsxwriter') as writer:
                data.to_excel(writer, index=False)

        Email.send_mail_with_attachment(
            receiver="nam.mk@urbox.vn",
            subject="test subject",
            content="test_content",
            attachments=[
                {
                    "title": order_list_filename,
                    "path": f"{daily_report_path}/{order_list_filename}"
                }
            ]
        )

        # # report_date = report_date.strftime('%d%m%Y')
        # order_list_cancel = pandas.read_sql(query, redshift_connection)
        # redshift_connection.commit()
        # redshift_connection.close()
        # order_list_cancel.to_excel(f"{STORAGE_DIR}/vna_report/{report_date_filename}/nammk_test.xlsx", index=False)
        # # Email.send_mail_with_attachment("nam.mk@urbox.vn", "test subject", "test_content", ['nammk_test.csv'])
        # # # with open("test.csv", "w") as file:
        # # #     file.write(order_list_cancel.to_csv(index=False))
        # # # file.close()
        # #
        # # # s = sftp.Connection(host=host, username='urbox', password='Jul#020721Evoucher')
        # # # remote_path = "/urbox_data/"
        # # #
        # # # file = f"VNA_Evoucher_Orderlist_Cancel/VNA_Evoucher_Orderlist_Cancel_{report_date}{ext}
        # # # with s.open(remote_path + file, "w") as f:
        # # #     f.write(order_list_cancel.to_csv(index=False))
        # # # s.close()
    except Exception as e:
        print(e)
