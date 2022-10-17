from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from multi_source.code_tu_up_ma.param import *
from Config import *
import pandas as pd
import numpy as np
import re
import unidecode
import time

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


def get_cart_detail_id():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name_code_tu_up_ma)
    hash_code = tuple(df["code_decryption"].tolist())
    df_cart_detail = pd.read_sql_query(sql=get_cart_detail_by_hash_code.format(hash_code=hash_code),
                                       con=redshift_connection)
    df_merge = df.merge(df_cart_detail, how="left", left_on="code_decryption", right_on="code").fillna(0)
    # Update cart_detail_id in GSheet: hard_code J2
    grid_range_to_update_2 = f"{sheet_name_code_tu_up_ma}!J2"
    GSheetApi.update_value(list_result=df_merge[["cart_detail_id"]].values.tolist(),
                           grid_range_to_update=grid_range_to_update_2, gsheet_id=gsheet_id)

    df_final_to_import = df_merge[
        ["id", "brand", "brand_id", "gift_detail_title", "gift_detail_price", "brand_code", "using_time",
         "code_decryption", "cart_detail_id"]].fillna(0)

    columns_type_int = ['gift_detail_price', 'cart_detail_id']

    for column_type_int in columns_type_int:
        df_final_to_import[column_type_int].replace(to_replace=",", value="", inplace=True, regex=True)
        df_final_to_import[column_type_int] = df_final_to_import[column_type_int].astype('int64')
    df_final_to_import["using_time"] = pd.to_datetime(df_final_to_import.using_time).fillna("")
    df_final_to_import["using_time"] = df_final_to_import["using_time"].astype("str")
    df_final_to_import.drop_duplicates(subset=['brand_id', 'brand_code'], keep='first', inplace=True)
    df_final_to_import.reset_index(drop=True, inplace=True)
    return df_final_to_import


def code_tu_up_ma_to_redshift():
    df = get_cart_detail_id().copy()
    sentry_db_session.execute("TRUNCATE TABLE sentry.code_tu_up_ma")
    sentry_db_session.commit()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.code_tu_up_ma")
    redshift_db_session.commit()
    df.to_sql("code_tu_up_ma", con=sentry_connection, if_exists='append', index=False,
              schema='sentry')
    time.sleep(120)
    redshift_db_session.execute(QUERY_TO_UB_RAWDATA_URBOX_CODE_TU_AP_MA)
    redshift_db_session.commit()


if __name__ == "__main__":
    # get_cart_detail_id()
    code_tu_up_ma_to_redshift()
