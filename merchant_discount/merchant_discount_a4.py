from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
import pandas.io.sql as psql
from multi_source.fuction import GSheetApi
from merchant_discount.params_a4 import *
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
print(REDSHIFT_URI)
print(SENTRY_URI)
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


def get_discount_rate_chiet_khau_phang():
    print("joy xinh")


def get_discount_rate_type_5(df_discount_info: object, brand_id: str, accumulated_amount: int, cart_detail_money: int):
    df = df_discount_info[df_discount_info['id'] == brand_id].reset_index(drop=True)
    info = {}
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
        # Trường hợp cart_detail_id ở ranh giới giữa các mốc, lấy mốc có số tiền chênh lệch nhiều hơn
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
    # 1.1. Get discount info
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_DISCOUNT_INFO.format(calculate_type='5'),
        con=redshift_connection
    )

    # 1.2 Get cart_detail and accumulated amount từ các brand type 5

    df_cart_detail = pd.read_sql_query(sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(calculate_type='5'),
                                       con=redshift_connection)
    column_type_int = ['cart_detail_money', 'accumulated_amount_by_cart_detail']
    column_type_str = ['id']
    df_cart_detail[column_type_int] = df_cart_detail[column_type_int].astype('int64')
    df_cart_detail[column_type_str] = df_cart_detail[column_type_str].astype('str')

    # 1.3 Get cart_detail discount rate

    df_cart_detail['discount_info'] = df_cart_detail.apply(
        lambda x: get_discount_rate_type_5(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_cart_detail'],
            cart_detail_money=x['cart_detail_money']
        ),
        axis=1
    )
    df_cart_detail['discount_rate'] = df_cart_detail.apply(
        lambda x: get_discount_rate_type_5(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_cart_detail'],
            cart_detail_money=x['cart_detail_money']
        ).get('discount_rate'),
        axis=1
    )

    # 1.4 To redshift
    df_final = df_cart_detail.copy()
    df_final['type'] = 'brand'
    df_final['calculate_type'] = 'type 5'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate_type', 'discount_period_start',
         'discount_period_end']].reset_index(drop=True)
    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_final


def get_cart_detail_discount_rate_type_2():
    # 1.1 get cart_detail from gift_detail: nếu 1 gift_detail vừa thuộc gift_detail vừa thuộc brand thì ưu tiên lấy theo gift_detail trước
    redshift_db_session.execute(QUERY_GET_CART_DETAIL_TYPE_2_BY_GIFT_DETAIL)
    redshift_db_session.commit()
    # 1.2 get cart_detail from brand:
    redshift_db_session.execute(QUERY_GET_CART_DETAIL_TYPE_2_BY_BRAND)
    redshift_db_session.commit()


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
    # 1.1. Get discount info
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_DISCOUNT_INFO.format(calculate_type=4),
        con=redshift_connection
    )
    # 1.2 Get cart_detail and accumulated amount từ các brand type 4

    df_cart_detail = pd.read_sql_query(sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(calculate_type='4'),
                                       con=redshift_connection)

    column_type_int = ['cart_detail_money', 'accumulated_amount_by_cart_detail', 'accumulated_amount_by_brand_id']
    column_type_str = ['id']
    df_cart_detail[column_type_int] = df_cart_detail[column_type_int].astype('int64')
    df_cart_detail[column_type_str] = df_cart_detail[column_type_str].astype('str')

    # Mục chạy 1.2, 1.3 để tăng performance
    # 1.2 Get total amount by brand_id: lấy ra các mốc accumulated amount của brand_id đó
    df_brand_accumulated_amount_level = df_cart_detail[
        ['id', 'accumulated_amount_by_brand_id']].drop_duplicates().reset_index(
        drop=True)
    # 1.3 Get brand_id discount_rate
    df_brand_accumulated_amount_level['discount_info'] = df_brand_accumulated_amount_level.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_brand_id']
        ),
        axis=1
    )
    df_brand_accumulated_amount_level['discount_rate'] = df_brand_accumulated_amount_level.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_brand_id']
        ).get('discount_rate'),
        axis=1
    )
    # 1.4 Get cart_detail discount rate
    df_final = df_cart_detail.merge(df_brand_accumulated_amount_level, how="left",
                                    on=['id', 'accumulated_amount_by_brand_id'])
    df_final['type'] = 'brand'
    df_final['calculate_type'] = 'type 4'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate_type', 'discount_period_start',
         'discount_period_end']].reset_index(drop=True)

    # sentry_db_session.execute("DELETE FROM sentry.cart_detail_discount_rate where `calculate type`  = 'type 4'")
    # sentry_db_session.commit()
    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_final


def get_discount_rate_type_9(supplier_id: str, accumulated_amount: int, df_discount_info: object = None):
    df = df_discount_info[df_discount_info['id'] == supplier_id].reset_index(drop=True)
    info = {}
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
    # type 9 only for supplier
    # 1.1. Get discount info
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_DISCOUNT_INFO.format(calculate_type=9),
        con=redshift_connection
    )
    # 1.2 Get cart_detail and accumulated amount từ các supplier type 9

    df_cart_detail = pd.read_sql_query(sql=QUERY_GET_CART_DETAIL_FROM_SUPPLIER.format(calculate_type='9'),
                                       con=redshift_connection)
    column_type_int = ['money', 'accumulated_amount']
    column_type_str = ['id']
    df_cart_detail[column_type_int] = df_cart_detail[column_type_int].astype('int64')
    df_cart_detail[column_type_str] = df_cart_detail[column_type_str].astype('str')

    # 1.2 Get total amount by supplier_id: lấy ra các mốc accumulated amount của supplier đó
    df_supplier_accumulated_amount_level = df_cart_detail[['id', 'accumulated_amount']].drop_duplicates().reset_index(
        drop=True)

    # 1.3 Get supplier discount_rate

    df_supplier_accumulated_amount_level['discount_info'] = df_supplier_accumulated_amount_level.apply(
        lambda x: get_discount_rate_type_9(
            df_discount_info=df_discount_info,
            supplier_id=x['id'],
            accumulated_amount=x['accumulated_amount']
        ),
        axis=1
    )
    df_supplier_accumulated_amount_level['discount_rate'] = df_supplier_accumulated_amount_level.apply(
        lambda x: get_discount_rate_type_9(
            df_discount_info=df_discount_info,
            supplier_id=x['id'],
            accumulated_amount=x['accumulated_amount']
        ).get('discount_rate'),
        axis=1
    )
    # 1.4 Get cart_detail discount rate
    df_final = df_cart_detail.merge(df_supplier_accumulated_amount_level, how="left", on=['id', 'accumulated_amount'])
    df_final['type'] = 'supplier'
    df_final['calculate_type'] = 'type 9'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate_type', 'discount_period_start',
         'discount_period_end']].reset_index(drop=True)
    for i in range(0, len(df_final), 1000):
        batch_df = df_final.loc[i:i + 999]
        batch_df.to_sql("cart_detail_discount_rate", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df_final


def get_cart_detail_discount_rate_type_10():
    # type 10 only for brand
    # 1.1. Get discount info
    df_discount_info = pd.read_sql_query(
        sql=QUERY_GET_DISCOUNT_INFO.format(calculate_type=10),
        con=redshift_connection
    )
    # 1.2 Get cart_detail and accumulated amount từ các brand type 4

    df_cart_detail = pd.read_sql_query(sql=QUERY_GET_CART_DETAIL_FROM_BRAND.format(calculate_type='10'),
                                       con=redshift_connection)

    column_type_int = ['cart_detail_money', 'accumulated_amount_by_cart_detail', 'accumulated_amount_by_brand_id',
                       'count_cart_detail_by_brand_id']
    column_type_str = ['id']
    df_cart_detail[column_type_int] = df_cart_detail[column_type_int].astype('int64')
    df_cart_detail[column_type_str] = df_cart_detail[column_type_str].astype('str')

    # lấy discount_rate của type 4 rồi xử lý tiếp
    df_cart_detail['discount_info'] = df_cart_detail.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_brand_id']
        ),
        axis=1
    )
    df_cart_detail['discount_rate'] = df_cart_detail.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_brand_id']
        ).get('discount_rate'),
        axis=1
    )
    # 1.3 trường hợp discount_rate là số tuyệt đối: (Hoàng anh confirm: count số đơn hàng, chia đều => mức discount, làm tròn sau dấu phẩy 2 chữ số)
    df_cart_detail['discount_rate'] = df_cart_detail.apply(
        lambda x: get_discount_rate_type_4(
            df_discount_info=df_discount_info,
            brand_id=x['id'],
            accumulated_amount=x['accumulated_amount_by_brand_id']
        ).get('discount_rate'),
        axis=1
    )

    # df_cart_detail.loc[5, 'discount_rate'] = '3500000'
    # viết như dòng dưới sẽ bị lỗi copy warning => sửa thành dòng trên
    # df_cart_detail['discount_rate'].loc[5] = '3500000'
    df_cart_detail['discount_rate'] = df_cart_detail.apply(
        lambda x: x['discount_rate'] if '%' in x[
            'discount_rate'] else f"{round(int(x['discount_rate'].replace('%', '')) / int(x['accumulated_amount_by_brand_id']) * 100, 2)}%",
        axis=1
    )
    # 1.4 Get cart_detail discount rate
    df_final = df_cart_detail.copy()
    df_final['type'] = 'brand'
    df_final['calculate_type'] = 'type 10'
    df_final = df_final[
        ['id', 'type', 'cart_detail_id', 'discount_info', 'discount_rate', 'calculate_type', 'discount_period_start',
         'discount_period_end']].reset_index(drop=True)
    # print(df_final)
    # sentry_db_session.execute("DELETE FROM sentry.cart_detail_discount_rate where `calculate type`  = 'type 4'")
    # sentry_db_session.commit()
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


def main():
    # step 1: đẩy dữ liệu từ file gsheet vào database
    get_gsheet_discount()
    get_discount_schedule_by_id()  # cập nhật kỳ chiết khấu

    # step 2: đẩy dữ liệu vào sentry
    get_cart_detail_discount_rate_type_9()
    get_cart_detail_discount_rate_type_5()
    get_cart_detail_discount_rate_type_4()
    get_cart_detail_discount_rate_type_10()

    # step 3: sync dữ liệu từ sentry => ub_rawdata
    time.sleep(120)
    redshift_db_session.execute(QUERY_SENTRY_TO_UB_RAWDATA)
    redshift_db_session.commit()

    # step 4: đẩy dữ liệu type 2 vào ub_rawdata
    get_cart_detail_discount_rate_type_2()

    # reformat discount_rate
    redshift_db_session.execute(
        "UPDATE ub_rawdata.cart_detail_discount_rate set discount_rate_reformat = cast(replace(discount_rate, '%', '') as FLOAT)/100")
    redshift_db_session.commit()


if __name__ == "__main__":
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    import time

    start_time = time.time()
    main()

    # get_gsheet_discount()
    # get_discount_schedule_by_id()
    # get_gsheet_discount()
    # k = datetime.today()
    # k1 = datetime.strptime('2022-12-31', '%Y-%m-%d')
    # print(k1)
    # print(k1 > k)
    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
