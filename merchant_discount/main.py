from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from merchant_discount.param import *
from Config import *
import pandas as pd
import numpy as np
import re
import unidecode
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
REDSHIFT_URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
SENTRY_URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"
redshift_connection = create_engine(REDSHIFT_URI)
sentry_connection = create_engine(SENTRY_URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_connection))
sentry_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))
GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)


def reformat_str(text: str):
    joy = re.sub(' +', ' ', text.lower())
    joy = joy.strip()
    joy = joy.replace(" ", "_")
    joy = unidecode.unidecode(joy)
    return joy


def column_reformat(df: object):
    columns_reformat = []
    for column in df.columns:
        column = reformat_str(column)
        columns_reformat.append(column)
    df.columns = columns_reformat
    return df


def column_type_int_reformat(df: object):
    df = df.replace(to_replace=",", value="", regex=True)
    df = df.apply(lambda x: x.str.strip()).replace('', np.nan)
    df = df.fillna(0)
    df = df.astype('int')
    return df


def to_date(str_date: str):
    if str_date == "":
        return ''
    else:
        return datetime.strptime(str_date, '%m/%d/%Y')


def get_discount_schedule(ky_tinh_ck_gan_nhat: datetime, repeak: float, doi_soat_hang_ngay: bool = False):
    k = 0
    date_diff = 0
    df = pd.DataFrame()
    while True and date_diff >= 0:
        if doi_soat_hang_ngay:  # kỳ chiết khấu hàng ngày
            last_discount_period_start = ky_tinh_ck_gan_nhat - timedelta(days=1) + k * timedelta(days=1)
            last_discount_period_end = last_discount_period_start + timedelta(days=1)
            date_diff = (datetime.today() - last_discount_period_end).days
            date_diff = (datetime.today() - last_discount_period_end).days
            last_discount_period_start = last_discount_period_start.strftime("%Y-%m-%d")
            last_discount_period_end = last_discount_period_end.strftime("%Y-%m-%d")
            data = {'last_discount_period_start': [last_discount_period_start],
                    'last_discount_period_end': [last_discount_period_end]}
            df_1 = pd.DataFrame.from_dict(data)
            df = pd.concat([df, df_1])

        elif float(repeak).is_integer():  # kỳ chiết khấu = repeak tháng : lấy đầu tháng cuối tháng
            period = ky_tinh_ck_gan_nhat + k * relativedelta(months=repeak)
            last_month_date = ky_tinh_ck_gan_nhat.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
            if ky_tinh_ck_gan_nhat == last_month_date:  # kỳ chiết khấu đầu tháng, cuối tháng
                last_discount_period_end = period.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
                last_discount_period_start = (last_discount_period_end - relativedelta(months=repeak - 1)).replace(
                    day=1)
            else:  # kỳ chiết khấu tại 1 ngày bất kỳ
                last_discount_period_end = period
                last_discount_period_start = last_discount_period_end - relativedelta(months=repeak) + timedelta(days=1)
            date_diff = (datetime.today() - last_discount_period_end).days
            last_discount_period_start = last_discount_period_start.strftime("%Y-%m-%d")
            last_discount_period_end = last_discount_period_end.strftime("%Y-%m-%d")
            data = {'last_discount_period_start': [last_discount_period_start],
                    'last_discount_period_end': [last_discount_period_end]}
            df_1 = pd.DataFrame.from_dict(data)
            df = pd.concat([df, df_1])
        else:  # kỳ chiết khấu nửa tháng: lấy từ đầu tháng-15 và 16 - cuối tháng hàng tháng
            period = ky_tinh_ck_gan_nhat + k * relativedelta(months=1)
            last_discount_period_end = period.replace(day=1) - timedelta(days=1)
            last_discount_period_start = period.replace(day=1) - timedelta(days=last_discount_period_end.day)
            time_range = {
                "range_1": {
                    "start": last_discount_period_start,
                    "end": last_discount_period_start + timedelta(days=14)
                },
                "range_2": {
                    "start": last_discount_period_start + timedelta(days=15),
                    "end": last_discount_period_end
                }
            }
            for key, value in time_range.items():
                last_discount_period_start = value.get("start")
                last_discount_period_end = value.get("end")
                date_diff = (datetime.today() - last_discount_period_end).days
                last_discount_period_start = last_discount_period_start.strftime("%Y-%m-%d")
                last_discount_period_end = last_discount_period_end.strftime("%Y-%m-%d")
                data = {'last_discount_period_start': [last_discount_period_start],
                        'last_discount_period_end': [last_discount_period_end]}
                df_1 = pd.DataFrame.from_dict(data)
                df = pd.concat([df, df_1])
        k = k + 1
    return df


def get_discount_schedule_by_id():
    query = """
    SELECT 
        DISTINCT         
        merchant_discount_test.ky_doi_soat_tu,
        merchant_discount_test.repeak,
        merchant_discount_test.ky_tinh_ck_gan_nhat
        from sentry.merchant_discount_test
        WHERE
        merchant_discount_test.ky_tinh_ck_gan_nhat is not NULL
    """
    df_discount = pd.read_sql_query(sql=query, con=redshift_connection)
    final_df = pd.DataFrame()
    for i in df_discount.index:
        ky_tinh_ck_gan_nhat = df_discount['ky_tinh_ck_gan_nhat'].loc[i]
        repeak = float(df_discount['repeak'].loc[i])
        ky_doi_soat_tu = df_discount['ky_doi_soat_tu'].loc[i]
        if "N" in ky_doi_soat_tu:
            status = True
        else:
            status = False
        df = get_discount_schedule(ky_tinh_ck_gan_nhat=ky_tinh_ck_gan_nhat, repeak=repeak, doi_soat_hang_ngay=status)
        df['ky_doi_soat_tu'] = ky_doi_soat_tu
        df['repeak'] = repeak
        df['ky_tinh_ck_gan_nhat'] = ky_tinh_ck_gan_nhat
        final_df = pd.concat([final_df, df])
    query_2 = """
    SELECT
        DISTINCT
        merchant_discount_test.id,
        merchant_discount_test.type,
        merchant_discount_test.ky_doi_soat_tu,
        merchant_discount_test.repeak,
        merchant_discount_test.ky_tinh_ck_gan_nhat
        from sentry.merchant_discount_test
        WHERE
        merchant_discount_test.ky_tinh_ck_gan_nhat is not NULL
    """
    df_id = pd.read_sql_query(sql=query_2, con=redshift_connection)
    columns_type_float = ['repeak']
    for column_type_float in columns_type_float:
        df_id[column_type_float] = df_id[column_type_float].astype(np.float64)

    df_discount_schedule_by_id = df_id.merge(final_df, how="left",
                                             on=['ky_doi_soat_tu', 'repeak', 'ky_tinh_ck_gan_nhat'])
    df_discount_schedule_by_id = df_discount_schedule_by_id.sort_values(
        ['id', 'last_discount_period_start'],
        ascending=True
    ).reset_index(drop=True)
    sentry_db_session.execute("TRUNCATE TABLE sentry.discount_schedule_by_id")
    sentry_db_session.commit()
    for i in range(0, len(df_discount_schedule_by_id), 1000):
        batch_df = df_discount_schedule_by_id.loc[i:i + 999]
        batch_df.to_sql("discount_schedule_by_id", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))


def get_gsheet_discount():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name)
    df = column_reformat(df=df)
    columns_type_int = ['money_min']
    for column_type_int in columns_type_int:
        df[column_type_int].replace(to_replace=",", value="", inplace=True, regex=True)
        df[column_type_int].replace(to_replace="", value="0", inplace=True, regex=True)
        df[column_type_int] = df[column_type_int].astype('int64')
    columns_type_float = ['repeak']
    for column_type_float in columns_type_float:
        df[column_type_float].replace(to_replace="%", value="", inplace=True, regex=True)
        df[column_type_float] = df[column_type_float].astype(np.float64)
    df['ky_tinh_ck_gan_nhat'] = df['ky_tinh_ck_gan_nhat'].apply(lambda x: to_date(x))
    df['ky_tinh_ck_gan_nhat'] = df['ky_tinh_ck_gan_nhat'].replace('', np.nan)
    # column_type_int = ['cart_detail_money', 'accumulated_amount_by_cart_detail', 'accumulated_amount_by_brand_id']
    # column_type_str = ['id']
    # df_cart_detail[column_type_int] = df_cart_detail[column_type_int].astype('int64')
    # df_cart_detail[column_type_str] = df_cart_detail[column_type_str].astype('str')
    print(df)
    # sentry_db_session.execute("TRUNCATE TABLE sentry.merchant_discount_test")
    # sentry_db_session.commit()
    # for i in range(0, len(df), 1000):
    #     batch_df = df.loc[i:i + 999]
    #     batch_df.to_sql("merchant_discount_test", con=sentry_connection, if_exists='append', index=False,
    #                     schema='sentry')
    #     print(batch_df.tail(3))


def get_discount_rate_type_5(df_2: object, brand_id: str, accumulated_amount: int, cart_detail_money: int):
    df = df_2[df_2['id'] == brand_id].reset_index(drop=True)
    for k in range(0, df.index.stop):
        if k == 0:
            info = {
                "brand_id": brand_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": int(df['money_min'].loc[k + 1]),
                "discount_rate": df['discount_rate'].loc[k],
                "pre_discount_rate": df['discount_rate'].loc[k],
                "cart_detail_money": cart_detail_money
            }

        elif k == df.index.stop - 1:
            info = {
                "brand_id": brand_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": np.inf,
                "discount_rate": df['discount_rate'].loc[k],
                "pre_discount_rate": df['discount_rate'].loc[k - 1],
                "cart_detail_money": cart_detail_money
            }
        else:
            info = {
                "brand_id": brand_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": int(df['money_min'].loc[k + 1]),
                "discount_rate": df['discount_rate'].loc[k],
                "pre_discount_rate": df['discount_rate'].loc[k - 1],
                "cart_detail_money": cart_detail_money
            }
        if info.get('range_start') <= accumulated_amount <= info.get('range_stop'):
            diff_money = accumulated_amount - info.get('range_start')
            if diff_money > info.get('cart_detail_money') / 2:
                pass
            else:
                discount_rate = {"discount_rate": info.get('pre_discount_rate')}
                info.update(discount_rate)
            break
    return info


def get_cart_detail_discount_rate_type_5():
    # type 5 only for brand
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_THRESOLD.format(calculate_type=5),
        con=redshift_connection
    )
    df_brand = df_discount_info[
        ['id', 'last_discount_period_start',
         'last_discount_period_end']].drop_duplicates().reset_index(drop=True)
    df_1 = pd.DataFrame()
    for i in df_brand.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(brand_id=df_brand['id'].loc[i],
                                                        start_date=df_brand['last_discount_period_start'].loc[i],
                                                        end_date=df_brand['last_discount_period_end'].loc[i]
                                                        ),
            con=redshift_connection
        )
        df_1 = pd.concat([df_1, df])
    columns_type_str = ['id']
    df_1[columns_type_str] = df_1[columns_type_str].astype('str')
    df_1['discount_info'] = df_1.apply(
        lambda x: get_discount_rate_type_5(
            df_2=df_discount_info,
            brand_id=x['id'],
            cart_detail_money=x['money'],
            accumulated_amount=x['accumulated_amount']
        ),
        axis=1
    )
    df_1['discount_rate'] = df_1.apply(
        lambda x: get_discount_rate_type_5(
            df_2=df_discount_info,
            brand_id=x['id'],
            cart_detail_money=x['money'],
            accumulated_amount=x['accumulated_amount']
        ).get('discount_rate'),
        axis=1
    )
    df_1['type'] = 'brand'
    df_1['calculate type'] = 'type 5'
    df_1 = df_1[['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate type']].reset_index(
        drop=True)
    for i in range(0, len(df_1), 1000):
        batch_df = df_1.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_1


def get_cart_detail_discount_rate_type_2():
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_THRESOLD.format(calculate_type=2),
        con=redshift_connection
    )
    # 1.1: map qua brand
    df_1 = pd.DataFrame()
    df_brand = df_discount_info[df_discount_info['type'] == 'brand'][
        ['id', 'type', 'last_discount_period_start',
         'last_discount_period_end', 'discount_rate']].drop_duplicates().reset_index(drop=True)
    # df_brand = df_brand[df_brand['id'] == '243']
    # print(df_brand)
    for i in df_brand.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(brand_id=df_brand['id'].loc[i],
                                                        start_date=df_brand['last_discount_period_start'].loc[i],
                                                        end_date=df_brand['last_discount_period_end'].loc[i]
                                                        ),
            con=redshift_connection
        )
        df['id'] = df['id'].astype('str')
        df_1 = pd.concat([df_1, df])
    df_1 = df_1.merge(df_brand, how="left", on=['id'])
    # 1.2: map qua gift_detail
    df_2 = pd.DataFrame()
    df_gift_detail_id = df_discount_info[df_discount_info['type'] == 'gift_detail'][
        ['id', 'type', 'last_discount_period_start',
         'last_discount_period_end', 'discount_rate']].drop_duplicates().reset_index(drop=True)
    for i in df_gift_detail_id.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_GIFT_DETAIL.format(
                gift_detail_id=df_gift_detail_id['id'].loc[i],
                start_date=df_gift_detail_id['last_discount_period_start'].loc[i],
                end_date=df_gift_detail_id['last_discount_period_end'].loc[i]
            ),
            con=redshift_connection
        )
        df['id'] = df['id'].astype('str')
        df_2 = pd.concat([df_2, df])
    df_2 = df_2.merge(df_gift_detail_id, how="left", on=['id'])

    # 1.3: map qua gift
    df_3 = pd.DataFrame()
    df_gift_id = df_discount_info[df_discount_info['type'] == 'gift'][
        ['id', 'type', 'last_discount_period_start',
         'last_discount_period_end', 'discount_rate']].drop_duplicates().reset_index(drop=True)

    for i in df_gift_id.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_GIFT.format(
                gift_id=df_gift_id['id'].loc[i],
                start_date=df_gift_id['last_discount_period_start'].loc[i],
                end_date=df_gift_id['last_discount_period_end'].loc[i]
            ),
            con=redshift_connection
        )
        df['id'] = df['id'].astype('str')
        df_3 = pd.concat([df_3, df])
    if not df_3.empty:
        df_3 = df_3.merge(df_gift_id, how="left", on=['id'])

    df_final = pd.concat([df_1, df_2, df_3]).reset_index(drop=True)
    df_final['discount_info'] = 'calculate type = 2'
    df_final['calculate type'] = 'type 2'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate type']].reset_index(
        drop=True)

    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_final


def get_discount_rate_type_4(df_discount_info: object, brand_id: str, accumulated_amount: int):
    df = df_discount_info[df_discount_info['id'] == brand_id].reset_index(drop=True)

    for k in range(0, df.index.stop):

        if k == df.index.stop - 1:
            info = {
                "brand_id": brand_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": np.inf,
                "discount_rate": df['discount_rate'].loc[k],
                "accumulated_amount": accumulated_amount
            }
        else:

            info = {
                "brand_id": brand_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": int(df['money_min'].loc[k + 1]),
                "discount_rate": df['discount_rate'].loc[k],
                "accumulated_amount": accumulated_amount
            }
        if info.get('range_start') <= accumulated_amount < info.get('range_stop'):
            break
    return info


def get_cart_detail_discount_rate_type_4():
    # type 4 only for brand
    # 1.1 Get cart_detail and accumulated amount
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_THRESOLD.format(calculate_type=4),
        con=redshift_connection
    )
    df_brand = df_discount_info[
        ['id', 'last_discount_period_start',
         'last_discount_period_end']].drop_duplicates().reset_index(drop=True)
    # df_brand = df_brand[df_brand['id'] == '36']
    df_1 = pd.DataFrame()
    for i in df_brand.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(brand_id=df_brand['id'].loc[i],
                                                        start_date=df_brand['last_discount_period_start'].loc[i],
                                                        end_date=df_brand['last_discount_period_end'].loc[i]
                                                        ),
            con=redshift_connection
        )
        df_1 = pd.concat([df_1, df])
    columns_type_str = ['id']
    df_1[columns_type_str] = df_1[columns_type_str].astype('str')

    # 1.2 Get total amount by brand id
    df_total_amount_by_brand_id = df_1.groupby('id').agg({'accumulated_amount': 'max'})[
        ['accumulated_amount']].reset_index()
    # 1.3 Get brand discount_rate

    df_total_amount_by_brand_id['discount_info'] = df_total_amount_by_brand_id.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount']
        ),
        axis=1
    )
    df_total_amount_by_brand_id['discount_rate'] = df_total_amount_by_brand_id.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount']
        ).get('discount_rate'),
        axis=1
    )
    # 1.4 Get catdetail discount rate
    df_final = df_1.merge(df_total_amount_by_brand_id, how="left",
                          on=['id'])
    df_final['type'] = 'brand'
    df_final['calculate type'] = 'type 4'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate type']].reset_index(drop=True)

    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_final


def get_discount_rate_type_9(df_discount_info: object, supplier_id: str, accumulated_amount: int):
    df = df_discount_info[df_discount_info['id'] == supplier_id].reset_index(drop=True)
    for k in range(0, df.index.stop):

        if k == df.index.stop - 1:
            info = {
                "supplier_id": supplier_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": np.inf,
                "discount_rate": df['discount_rate'].loc[k],
                "accumulated_amount": accumulated_amount
            }
        else:

            info = {
                "supplier_id": supplier_id,
                "range_start": int(df['money_min'].loc[k]),
                "range_stop": int(df['money_min'].loc[k + 1]),
                "discount_rate": df['discount_rate'].loc[k],
                "accumulated_amount": accumulated_amount
            }
        if info.get('range_start') <= accumulated_amount < info.get('range_stop'):
            break
    return info


def get_cart_detail_discount_rate_type_9():
    # type 4 only for brand
    # 1.1 Get cart_detail and accumulated amount
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_THRESOLD.format(calculate_type=9),
        con=redshift_connection
    )

    df_supplier = df_discount_info[
        ['id', 'last_discount_period_start',
         'last_discount_period_end']].drop_duplicates().reset_index(drop=True)
    df_1 = pd.DataFrame()
    for i in df_supplier.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_SUPPLIER.format(supplier_id=df_supplier['id'].loc[i],
                                                           start_date=df_supplier['last_discount_period_start'].loc[i],
                                                           end_date=df_supplier['last_discount_period_end'].loc[i]
                                                           ),
            con=redshift_connection
        )
        df_1 = pd.concat([df_1, df])
    columns_type_str = ['id']
    df_1[columns_type_str] = df_1[columns_type_str].astype('str')

    # 1.2 Get total amount by supplier_id 
    df_total_amount_by_supplier_id = df_1.groupby('id').agg({'accumulated_amount': 'max'})[
        ['accumulated_amount']].reset_index()

    # 1.3 Get supplier discount_rate

    df_total_amount_by_supplier_id['discount_info'] = df_total_amount_by_supplier_id.apply(
        lambda x: get_discount_rate_type_9(
            df_discount_info=df_discount_info,
            supplier_id=x['id'],
            accumulated_amount=x['accumulated_amount']
        ),
        axis=1
    )
    df_total_amount_by_supplier_id['discount_rate'] = df_total_amount_by_supplier_id.apply(
        lambda x: get_discount_rate_type_9(
            df_discount_info=df_discount_info,
            supplier_id=x['id'],
            accumulated_amount=x['accumulated_amount']
        ).get('discount_rate'),
        axis=1
    )
    # 1.4 Get cart_detail discount rate
    df_final = df_1.merge(df_total_amount_by_supplier_id, how="left",
                          on=['id'])
    df_final['type'] = 'brand'
    df_final['calculate type'] = 'type 9'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate type']].reset_index(drop=True)
    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_final


def get_cart_detail_discount_rate_type_10():
    # type 4 only for brand
    # 1.1 Get cart_detail and accumulated amount
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_THRESOLD.format(calculate_type=10),
        con=redshift_connection
    )
    df_brand = df_discount_info[
        ['id', 'last_discount_period_start',
         'last_discount_period_end']].drop_duplicates().reset_index(drop=True)
    df_1 = pd.DataFrame()
    for i in df_brand.index:
        df = pd.read_sql_query(
            sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(brand_id=df_brand['id'].loc[i],
                                                        start_date=df_brand['last_discount_period_start'].loc[i],
                                                        end_date=df_brand['last_discount_period_end'].loc[i]
                                                        ),
            con=redshift_connection
        )
        df_1 = pd.concat([df_1, df])
    columns_type_str = ['id']
    df_1[columns_type_str] = df_1[columns_type_str].astype('str')
    df_1['type'] = 'brand'
    df_1['discount_info'] = 'none'
    df_1['discount_rate'] = 10
    df_1['calculate type'] = 10
    df_final = df_1[['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate type']].reset_index(
        drop=True)
    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))

    return df_final


def get_cart_redemption_time_range():
    # today = datetime.today()
    today = datetime.strptime('2019-08-31', '%Y-%m-%d')
    period = today - relativedelta(months=2)
    last_discount_period_end = today.replace(day=1) + relativedelta(months=1) - timedelta(days=1)
    last_discount_period_start = period.replace(day=1)

    print(last_discount_period_start, "---", last_discount_period_end)


if __name__ == "__main__":
    # get_cart_redemption_time_range()
    get_gsheet_discount()
    # get_discount_schedule(ky_tinh_ck_gan_nhat=datetime.strptime('2019-08-31', '%Y-%m-%d'), repeak=1, doi_soat_hang_ngay=False)
    # get_discount_schedule_by_id()

    # get_cart_detail_discount_rate_type_5()
    # get_cart_detail_discount_rate_type_2()
    # get_cart_detail_discount_rate_type_4()
    # get_cart_detail_discount_rate_type_9()
    # get_cart_detail_discount_rate_type_10()

    # get_discount_rate_type_4(brand_id='1', accumulated_amount=3500000000)

    # get_gsheet_discount()
    # k = datetime.today()
    # k1 = datetime.strptime('2022-12-31', '%Y-%m-%d')
    # print(k1)
    # print(k1 > k)
