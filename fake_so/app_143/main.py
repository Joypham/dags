from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from sqlalchemy import create_engine
import pandas as pd
import unidecode
import codecs
from sqlalchemy.orm import scoped_session, sessionmaker
from Config import *
from fake_so.app_143.param import *
import time
from datetime import datetime, timedelta
import random

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)

# https://docs.google.com/spreadsheets/d/1_hEkr5VwM-LVBHQTySyyl1_l8qDxcYECrB0f4Aj5DqU/edit#gid=237215637&fvid=1945307941
REDSHIFT_URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
SENTRY_URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"

GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)
redshift_conection = create_engine(REDSHIFT_URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_conection))
sentry_connection = create_engine(SENTRY_URI)
sentry_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))


def get_cart_detail_created(df_percentile_time: object, card_created: str, gift_id: str):
    df_cart_detail_created = df_percentile_time[df_percentile_time['gift_id'] == gift_id].reset_index(drop=True)
    pct_25 = int(df_cart_detail_created['percentile_25'].loc[0])
    pct_75 = int(df_cart_detail_created['percentile_75'].loc[0])
    second = random.randint(pct_25, pct_75)
    card_created = datetime.strptime(card_created, '%Y-%m-%d %H:%M:%S')
    cart_detail_created = card_created + timedelta(seconds=second)
    # cart_detail_created = datetime.strftime(cart_detail_created, '%Y-%m-%d %H:%M:%S')

    return cart_detail_created


def get_cart_detail_to_fake():
    df_percentile_time = pd.read_sql_query(sql=QUERY_GET_PERCENTILE, con=redshift_conection)
    # Lấy ra các thẻ không có giao dịch chi tiêu
    df_card_no_trans = pd.read_sql_query(sql=QUERY_GET_CARD_NO_TRANS, con=redshift_conection)

    # Lấy random giao dich
    df_fake_cart_detail_holistics = pd.read_sql_query(sql=QUERY_GET_RANDOM_CART_DETAIL, con=redshift_conection)
    df_fake_cart_detail_holistics = df_card_no_trans.join(df_fake_cart_detail_holistics)
    df_fake_cart_detail_holistics['card_created'] = df_fake_cart_detail_holistics['card_created'].astype('str')
    # fake thoi gian doi
    df_fake_cart_detail_holistics['cart_detail_created_at'] = df_fake_cart_detail_holistics.apply(
        lambda x: get_cart_detail_created(df_percentile_time=df_percentile_time, card_created=x['card_created'],
                                          gift_id=str(x['gift_id'])), axis=1)
    # update lai using_time
    df_fake_cart_detail_holistics['using_time'] = df_fake_cart_detail_holistics.apply(
        lambda x: x['cart_detail_created_at'].date(), axis=1)

    # drop card_created
    df_fake_cart_detail_holistics.drop('card_created', axis=1, inplace=True)
    redshift_db_session.execute("TRUNCATE TABLE fake_so.cart_detail_holistics")
    redshift_db_session.commit()

    for i in range(0, len(df_fake_cart_detail_holistics), 10):
        batch_df = df_fake_cart_detail_holistics.loc[i:i + 9]
        batch_df.to_sql("cart_detail_holistics", con=redshift_conection, if_exists='append', index=False,
                        schema='fake_so')
        print(batch_df.tail(3))

    # Card_id thuộc danh sách fake phải update lại money_consuming và money_remain:


if __name__ == "__main__":
    start_time = time.time()
    # get_cart_detail_created("2022-01-07 09:46:23", "1553")
    get_cart_detail_to_fake()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
