from Config import *

import pandas
import psycopg2

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)


def main(report_date):
    try:
        query = f"""
            CALL ub_ods.pcd_daily_export_revenue_data_vna_cancel('{report_date}', '{report_date}', 'tmp_table');
            SELECT * FROM tmp_table;
        """
        # report_date = report_date.strftime('%d%m%Y')
        order_list_cancel = pandas.read_sql(query, redshift_connection)
        redshift_connection.commit()
        print(order_list_cancel)
        # redshift_cursor = redshift_connection.cursor()
        # redshift_cursor.execute(query)
        # order_list_cancel = redshift_cursor.fetchall()
        # redshift_cursor.close()

        with open("test.csv", "w") as file:
            file.write(order_list_cancel.to_csv(index=False))
        file.close()

        # s = sftp.Connection(host=host, username='urbox', password='Jul#020721Evoucher')
        # remote_path = "/urbox_data/"
        #
        # file = f"VNA_Evoucher_Orderlist_Cancel/VNA_Evoucher_Orderlist_Cancel_{report_date}{ext}
        # with s.open(remote_path + file, "w") as f:
        #     f.write(order_list_cancel.to_csv(index=False))
        # s.close()
    except Exception as e:
        print(e)
