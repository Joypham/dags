from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from multi_source.cs_data_trainning.param import *
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


def cs_data_tranning_to_redshift():
    '''
        https://docs.google.com/spreadsheets/d/1DhVU62XoDooouIVm1U45h5L_vmOUDVr2aQ8ufDVjRGs/edit#gid=178407406
        sheet_name: Data trainning
        đọc đến dòng có timestamp cuối cùng
    '''
    df = GSheetApi.get_df_from_speadsheet(gsheet_id='1DhVU62XoDooouIVm1U45h5L_vmOUDVr2aQ8ufDVjRGs',
                                          sheet_name='Data training')
    df.rename(columns=rename_columns, inplace=True)
    column_reformat = []
    for key, value in rename_columns.items():
        column_reformat.append(value)
        df.rename(columns={key: value}, inplace=True)
    df = df[column_reformat]
    df = df.astype('str')
    sentry_db_session.execute("TRUNCATE TABLE sentry.cs_data_tranning")
    sentry_db_session.commit()
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.cs_data_tranning")
    redshift_db_session.commit()
    for i in range(0, len(df), 1000):
        batch_df = df.loc[i:i + 999]
        batch_df.to_sql("cs_data_tranning", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    time.sleep(120)
    redshift_db_session.execute(QUERY)
    redshift_db_session.commit()


if __name__ == "__main__":
    cs_data_tranning_to_redshift()
