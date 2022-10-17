from sai_so_du.param import *
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from Config import *
import pandas as pd
import numpy as np

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
REDSHIFT_URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
SENTRY_URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"
redshift_connection = create_engine(REDSHIFT_URI)
sentry_connection = create_engine(SENTRY_URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_connection))
sentry_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))
GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)


def get_diff_card_money_remain():
    df_diff_card_money_remain = pd.read_sql_query(
        sql=query_get_card_diff_money_remain,
        con=redshift_connection
    )
    return df_diff_card_money_remain


def by_dup_card_trans():
    df_by_dup_card_trans = pd.read_sql_query(
        sql=query_by_dup_card_trans,
        con=redshift_connection
    )
    df_by_dup_card_trans['notes'] = 'dup card_trans'
    return df_by_dup_card_trans


def by_dup_cart():
    df_by_dup_cart = pd.read_sql_query(
        sql=query_by_dup_cart,
        con=redshift_connection
    )
    df_by_dup_cart['notes'] = 'dup cart'
    return df_by_dup_cart


def main():
    # step 1: lấy ra danh sách thẻ bị lệch số tiền money_remain
    df_card = get_diff_card_money_remain()
    # step 2: lấy ra giao dịch bị duplicate trong bảng card_trans và cart
    df1 = by_dup_card_trans()
    df2 = by_dup_cart()
    df = pd.concat([df1, df2])
    df.drop_duplicates(subset=['card_id', 'cart_id', 'cart_detail_created'], keep='first', inplace=True,
                       ignore_index=True)
    # step 3: lấy ra các thẻ vừa bị lệch money_remain vừa bị dup giao dịch
    df_merge = df_card.merge(df, how="inner", on='card_id').fillna(0)
    # Lấy ra report tương ứng
    df_card_report = df_merge[
        ['card_id', 'version', 'money_remain', 'calculate_money_remain', 'diff', 'so_lan_lap', 'money',
         'notes']].drop_duplicates(ignore_index=True)
    print(df_card_report)
    GSheetApi.creat_new_sheet_and_update_data_from_df(df=df_card_report.astype('str'),
                                                      gsheet_id="1g0XpMA_43WlrbTCsSPLb6K8GZZCMbuVCkTh_oIzmIPY",
                                                      new_sheet_name="Báo cáo thẻ dup 15.08.2022")
    # df_transaction_report = df_merge
    # GSheetApi.creat_new_sheet_and_update_data_from_df(df=df_transaction_report.astype('str'),
    #                                                   gsheet_id="1g0XpMA_43WlrbTCsSPLb6K8GZZCMbuVCkTh_oIzmIPY",
    #                                                   new_sheet_name="Báo cáo chi tiết giao dịch 15.08.2022")


if __name__ == "__main__":
    import time

    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    start_time = time.time()
    # get_diff_card_money_remain()
    # by_dup_card_trans()
    main()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
