from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from sqlalchemy import create_engine
import pandas as pd
import unidecode
import codecs
from sqlalchemy.orm import scoped_session, sessionmaker
from Config import *
from fake_so.app_217.param import *
import time

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)

# https://docs.google.com/spreadsheets/d/1_hEkr5VwM-LVBHQTySyyl1_l8qDxcYECrB0f4Aj5DqU/edit#gid=237215637&fvid=1945307941
REDSHIFT_URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
SENTRY_URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"

GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)
redshift_conection = create_engine(REDSHIFT_URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_conection))
sentry_connection = create_engine(SENTRY_URI)
sentry_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))


def get_conversion_rate_by_gift_detail_id():
    db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_conection))
    db_session.execute("call ub_holistics.pcd_get_using_time();")
    results = db_session.execute(QUERY_GET_CONDITION_BY_GIFTDETAIL_ID)
    db_session.close()
    df = pd.DataFrame(results)
    df.columns = results.keys()
    db_session.execute("TRUNCATE TABLE fake_so.condition_by_gift_detail_id")
    db_session.commit()
    for i in range(0, len(df), 1000):
        batch_df = df.loc[i:i + 999]
        # CONDITION_BY_GIFTDETAIL_ID
        batch_df.to_sql("condition_by_gift_detail_id", con=redshift_conection, if_exists='append', index=False,
                        schema='fake_so')
        print(batch_df.tail(3))


def get_fake_num(no_change: int, conversion_rate: float, no_range: int, no_fake: int):
    num_to_fake = int(no_range * conversion_rate / 100)
    no_fake = no_fake + num_to_fake
    conversion_rate_after_fake = int(no_fake / no_change * 100)
    if conversion_rate_after_fake >= 95:
        num_to_fake = 0
    return num_to_fake


def get_cart_detail_to_fake():
    '''
        Diễn giải bài toán: required fake khoảng 70% chia 2 phần:
            - 60% -65%: ưu tiên fake theo tỷ lệ conversion (vòng lặp chạy đến khi nào đạt target_amount trong khoảng 60-65%
            - 5-10%: lấy random trong nhóm quà có conversion rate < 10%
            - vòng lặp fake số của từng gift_detail đảm bảo: conversion_rate của gift_detail < = 95%
    '''
    # brief data
    df_used_rate = pd.read_sql_query(sql=QUERY_GET_USED_RATE, con=redshift_conection)
    df_used_rate = df_used_rate.astype('int64')
    print(df_used_rate)
    changed_amount = df_used_rate['changed_amount'].loc[0]
    target_used_amount_range_1 = (70 / 100 * changed_amount, 75 / 100 * changed_amount)
    target_used_amount_range_2 = (2 / 100 * changed_amount, 10 / 100 * changed_amount)
    # Get num of gift_detail to fake
    # Step 1: Get num of gift_detail_id ưu tiên to fake (nhóm 1)

    df_conversion_rate_1 = pd.read_sql_query(sql=QUERY_GET_CONVERSION_RATE, con=redshift_conection)
    df_conversion_rate_1['no_fake'] = df_conversion_rate_1['no_used']
    df_conversion_rate_1['no_range'] = df_conversion_rate_1['no_change'] - df_conversion_rate_1['no_fake']
    while True:
        df_conversion_rate_1['num_to_fake'] = df_conversion_rate_1.apply(
            lambda x: get_fake_num(no_change=x['no_change'],
                                   conversion_rate=x['conversion_rate'],
                                   no_range=x['no_range'],
                                   no_fake=x['no_fake']),
            axis=1)

        df_conversion_rate_1['no_fake'] = df_conversion_rate_1['no_fake'] + df_conversion_rate_1['num_to_fake']
        df_conversion_rate_1['no_range'] = df_conversion_rate_1['no_change'] - df_conversion_rate_1['no_fake']
        df_conversion_rate_1['fake_amount'] = df_conversion_rate_1['no_fake'] * df_conversion_rate_1['price']
        fake_amount_1 = df_conversion_rate_1['fake_amount'].sum()
        check = target_used_amount_range_1[0] < fake_amount_1 < target_used_amount_range_1[1]
        if check:
            break
    df_conversion_rate_1 = df_conversion_rate_1[
        ['gift_detail_id', 'title', 'price', 'no_change', 'no_used', 'conversion_rate', 'no_fake', 'fake_amount']]

    # Step 2: Get num of gift_detail_id ko ưu tiên to fake (nhóm 2): chú ý conversion rate lấy ra ở phần này là fake
    #             CASE
    #                 WHEN no_used <> 0 THEN 3
    #                 WHEN price > 1000 THEN 2
    #                 ELSE 1
    #             END conversion_rate
    df_conversion_rate_2 = pd.read_sql_query(sql=QUERY_GET_LOW_CONVERSION_RATE, con=redshift_conection)
    df_conversion_rate_2['no_fake'] = df_conversion_rate_2['no_used']
    df_conversion_rate_2['no_range'] = df_conversion_rate_2['no_change'] - df_conversion_rate_2['no_fake']
    while True:
        df_conversion_rate_2['num_to_fake'] = df_conversion_rate_2.apply(
            lambda x: get_fake_num(no_change=x['no_change'],
                                   conversion_rate=x['conversion_rate'],
                                   no_range=x['no_range'],
                                   no_fake=x['no_fake']),
            axis=1)

        df_conversion_rate_2['no_fake'] = df_conversion_rate_2['no_fake'] + df_conversion_rate_2['num_to_fake']
        df_conversion_rate_2['no_range'] = df_conversion_rate_2['no_change'] - df_conversion_rate_2['no_fake']
        df_conversion_rate_2['fake_amount'] = df_conversion_rate_2['no_fake'] * df_conversion_rate_2['price']
        fake_amount_2 = df_conversion_rate_2['fake_amount'].sum()
        check = target_used_amount_range_2[0] < fake_amount_2 < target_used_amount_range_2[1]
        if check:
            break
    df_conversion_rate_2['conversion_rate'] = 0
    df_conversion_rate_2 = df_conversion_rate_2[
        ['gift_detail_id', 'title', 'price', 'no_change', 'no_used', 'conversion_rate', 'no_fake', 'fake_amount']]

    # Step 3: merged data
    df_conversion_rate = pd.concat([df_conversion_rate_1, df_conversion_rate_2]).reset_index(drop=True)
    df_conversion_rate['num_of_trans_to_fake'] = df_conversion_rate['no_fake'] - df_conversion_rate['no_used']
    fake_amount = df_conversion_rate['fake_amount'].sum()
    # print(df_conversion_rate)

    # Step 4: get_cart_detail_to_fake
    redshift_db_session.execute("TRUNCATE TABLE fake_so.num_of_trans_to_fake")
    redshift_db_session.commit()
    for i in range(0, len(df_conversion_rate), 1000):
        batch_df = df_conversion_rate.loc[i:i + 999]
        # CONDITION_BY_GIFTDETAIL_ID
        batch_df.to_sql("num_of_trans_to_fake", con=redshift_conection, if_exists='append', index=False,
                        schema='fake_so')
        print(batch_df.tail(3))


if __name__ == "__main__":
    start_time = time.time()
    # get_conversion_date_by_gift_detail_id()
    get_cart_detail_to_fake()


    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
