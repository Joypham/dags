from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from multi_source.PO.param import *
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
redshift_connection.dialect.description_encoding = None
sentry_connection = create_engine(SENTRY_URI)
sentry_connection.dialect.description_encoding = None
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


def sub_po_to_redshift():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name_sub_po)
    df = column_reformat(df=df)
    df = df.rename(columns=sub_po_dict_column_name)
    df = df[list(sub_po_dict_column_name.values())]
    df['sub_po_code_external'] = df['po_code_internal']
    column_type_int = ['volume', 'volume_after_discount', 'handover_money', 'money_in_system']
    df[column_type_int] = column_type_int_reformat(df=df[column_type_int])
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
    # # migrate data from redshift.sentry to ub_rawdata
    time.sleep(60)
    redshift_db_session.execute(QUERY_TO_UB_RAWDATA_URBOX_SUBPO)
    redshift_db_session.commit()


def po_to_redshift():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name_po)
    df = column_reformat(df=df)
    df = df.rename(columns=po_dict_column_name)
    df = df[list(po_dict_column_name.values())]
    column_type_int = ['volume_in_contract', 'volume_real', 'volume']
    df[column_type_int] = column_type_int_reformat(df=df[column_type_int])
    df.drop_duplicates()
    k = df[df['app_id'] == '585']
    sentry_db_session.execute("TRUNCATE TABLE sentry.urbox_po")
    sentry_db_session.commit()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.urbox_po")
    redshift_db_session.commit()
    for i in range(0, len(df), 100):
        batch_df = df.loc[i:i + 99]
        batch_df.to_sql("urbox_po", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    time.sleep(120)
    redshift_db_session.execute(QUERY_TO_UB_RAWDATA_URBOX_PO)
    redshift_db_session.commit()


def clients_to_redshift():
    df = GSheetApi.get_df_from_speadsheet(gsheet_id=gsheet_id, sheet_name=sheet_name_client_sub_name)
    df = column_reformat(df=df)
    df = df.rename(columns=client_sub_name_dict_column_name)
    df = df[list(client_sub_name_dict_column_name.values())]
    df.drop_duplicates()
    # print(df)
    sentry_db_session.execute("TRUNCATE TABLE sentry.tbl_client")
    sentry_db_session.commit()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.tbl_client")
    redshift_db_session.commit()
    for i in range(0, len(df), 100):
        batch_df = df.loc[i:i + 99]
        batch_df.to_sql("tbl_client", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    time.sleep(60)
    redshift_db_session.execute(QUERY_TO_UB_RAWDATA_URBOX_CLIENT_SUB_NAME)
    redshift_db_session.commit()


def call_pcd():
    raw_query = 'call ub_rawdata.pcd_urbox_po()'
    redshift_db_session.execute(raw_query)
    redshift_db_session.commit()


if __name__ == "__main__":
    sub_po_to_redshift()
    po_to_redshift()
    clients_to_redshift()
    call_pcd()
# po_to_redshift()
# sub_po_to_redshift()
