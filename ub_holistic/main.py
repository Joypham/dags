from tiki_alert.param import *
from Config import *
import re
import pandas as pd
from Email import Email
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

URI = f"redshift+redshift_connector://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
redshift_connection = create_engine(URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=redshift_connection))


def call_pcd(pcd: str, **kwargs):
    redshift_db_session.execute(pcd)
    redshift_db_session.commit()


if __name__ == "__main__":
    import time
    # raw_query = "SELECT * from urbox.card limit 10"
    # redshift_db_session.execute(raw_query)
    # pcd_1 = "call ub_holistics.pcd_holistics_cart_detail_loyalty();"
    # call_pcd(pcd=pcd_1)
#
#     start_time = time.time()
#     pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
#
#     print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
