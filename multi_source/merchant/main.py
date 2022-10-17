from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from multi_source.merchant.param import *
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


def merchant_info_to_redshift():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name_merchant_info)
    df = column_reformat(df=df)
    df = df.rename(columns=merchant_info_dict_column_name)
    df = df[list(merchant_info_dict_column_name.values())]
    df.drop_duplicates()
    sentry_db_session.execute("TRUNCATE TABLE sentry.urbox_subpo2")
    sentry_db_session.commit()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.urbox_subpo")
    redshift_db_session.commit()
    for i in range(0, len(df), 100):
        batch_df = df.loc[i:i + 99]
        batch_df.to_sql("urbox_subpo2", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))


    # # # migrate data from redshift.sentry to ub_rawdata
    # time.sleep(60)
    # redshift_db_session.execute(QUERY_TO_UB_RAWDATA_URBOX_SUBPO)
    # redshift_db_session.commit()


if __name__ == "__main__":
    merchant_info_to_redshift()
# po_to_redshift()
# sub_po_to_redshift()
