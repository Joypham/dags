from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from multi_source.app_name_to_use.param import *
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


def app_name_to_use_to_redshift():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name_app_name_to_use)
    df = column_reformat(df=df)
    df = df.rename(columns=app_name_to_use_dict_column_name)
    df = df[list(app_name_to_use_dict_column_name.values())]
    column_type_int = []
    df[column_type_int] = column_type_int_reformat(df=df[column_type_int])
    df.drop_duplicates()
    sentry_db_session.execute("TRUNCATE TABLE sentry.app_name_to_use")
    sentry_db_session.commit()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.app_name_to_use")
    redshift_db_session.commit()
    for i in range(0, len(df), 100):
        batch_df = df.loc[i:i + 99]
        batch_df.to_sql("app_name_to_use", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    # migrate data from redshift.sentry to ub_rawdata
    time.sleep(60)
    redshift_db_session.execute(QUERY_TO_UB_RAWDATA_URBOX_APP_NAME_TO_USE)
    redshift_db_session.commit()
    # print(df.head(10))


def call_pcd():
    raw_query = 'call ub_rawdata.pcd_app_name_to_use()'
    redshift_db_session.execute(raw_query)


if __name__ == "__main__":
    app_name_to_use_to_redshift()
    call_pcd()
