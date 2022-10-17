import json

import requests
import pandas as pd
import time
from Config import *
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
from multi_source.fuction import GSheetApi

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
REDSHIFT_URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
SENTRY_URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"
redshift_connection = create_engine(REDSHIFT_URI)
sentry_connection = create_engine(SENTRY_URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_connection))
sentry_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))


class DanhSachAPI:
    # - Danh sách liên hệ. GET: https://urbox.1office.vn/api/customer/contact/gets
    # - Danh sách cơ hội. GET: https://urbox.1office.vn/api/customer/deal/gets
    # - Danh sách khách hàng. GET: https://urbox.1office.vn/api/customer/customer/gets
    lien_he = {"url": "https://urbox.1office.vn/api/customer/contact/gets", "table": "1office_lien_he"}
    co_hoi = {"url": "https://urbox.1office.vn/api/customer/deal/gets", "table": "1office_co_hoi"}
    khach_hang = {"url": "https://urbox.1office.vn/api/customer/customer/gets", "table": "1office_khach_hang"}


def get_data_from_page_num(page_num: int, url: str):
    response = requests.get(
        url=url,
        params={'access_token': '82776877162a06213b2d62388810751',
                'page': page_num}
    )
    response = response.json()
    data = response.get("data")
    df = pd.DataFrame()
    for x in data:
        list_value = []
        list_columns = []
        for key, value in x.items():
            list_value.append(value)
            list_columns.append(key)
        df = pd.concat([df, pd.DataFrame([list_value], columns=list_columns).fillna('')]).reset_index(drop=True)
    return df


def main_get_data_from_api(api_name: DanhSachAPI):
    count = 1
    df = pd.DataFrame()
    while True:
        k = get_data_from_page_num(page_num=count, url=api_name.get("url"))
        if not k.empty:
            df = pd.concat([df, k]).reset_index(drop=True)
            count = count + 1

        else:
            break
        print(count)
    df = df.astype('str')
    sentry_db_session.execute(f"TRUNCATE TABLE sentry.{api_name.get('table')}")
    sentry_db_session.commit()
    for i in range(0, len(df), 100):
        batch_df = df.loc[i:i + 99]
        batch_df.to_sql(api_name.get('table'), con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    return df


if __name__ == "__main__":
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    start_time = time.time()
    main_get_data_from_api(api_name=DanhSachAPI.khach_hang)
    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
