from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from multi_source.anh_khoi_request.param import *
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


def to_redshift():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name)
    df = column_reformat(df=df)
    df = df.rename(columns=sheet_name_column_name)
    df = df.drop_duplicates()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.anh_khoi_request")
    redshift_db_session.commit()
    for i in range(0, len(df), 10):
        batch_df = df.loc[i:i + 9]
        batch_df.to_sql("anh_khoi_request", con=redshift_connection, if_exists='append', index=False,
                        schema='ub_rawdata')
        print(batch_df.tail(3))


def map_app_536():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id='1xPEOBLzPBeTXSaLeH07FUx1O8l7o_LvdvHa80rO-Qr8', sheet_name='Sheet1')
    df.fillna(0, inplace=True)
    df = df.drop_duplicates()
    for i in range(0, len(df), 1000):
        batch_df = df.loc[i:i + 999]
        batch_df.to_sql("map_app_536", con=redshift_connection, if_exists='append', index=False,
                        schema='ub_rawdata')
        print(batch_df.tail(3))


def khoa_the_AIA():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id='16c3lujVOQSQylYFjzRJdVgUfFhfXKPX7H1y6-8BLakc',
                                          sheet_name='data_raw')
    df.fillna(0, inplace=True)
    df = df.drop_duplicates()
    # print(df)
    for i in range(0, len(df), 1000):
        batch_df = df.loc[i:i + 999]
        batch_df.to_sql("khoa_the_aia", con=redshift_connection, if_exists='append', index=False,
                        schema='ub_rawdata')
        print(batch_df.tail(3))


if __name__ == "__main__":
    to_redshift()
    # joy()
    # khoa_the_AIA()
