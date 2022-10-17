from core.redshift.urbox_redshift_connector.sql_alchemy_engine.URI import SQLALCHEMY_DATABASE_URI
from core.gsheet_api.function_new import GSheetApi
from core.gsheet_api import gsheet_cridential_path_user_urbox, gsheet_token_path_user_urbox
from sqlalchemy import create_engine
import pandas as pd
from ortools.sat.python import cp_model
import numpy as np

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
# https://docs.google.com/spreadsheets/d/1_hEkr5VwM-LVBHQTySyyl1_l8qDxcYECrB0f4Aj5DqU/edit#gid=1661669589
gsheet_id = '1_hEkr5VwM-LVBHQTySyyl1_l8qDxcYECrB0f4Aj5DqU'
sheet_name = 'data_final test'
GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)

urbox_engine = create_engine(SQLALCHEMY_DATABASE_URI(schema_name="urbox"))


def get_view():
    # fake 50% money_remain
    raw_query_1 = """
        with cte as (
            SELECT 
				gift_detail_id,
				COUNT(*) AS count_num								
            FROM ub_holistics.card_holistic
                JOIN urbox.gift_set ON gift_set.gift_id = card_holistic.gift_set_id AND gift_set.status = 2
                JOIN urbox.gift ON gift.id = gift_set.gift_id AND gift.status = 1
                JOIN urbox.gift_detail ON gift_set.gift_detail_id = gift_detail.id AND gift_detail.status = 1 AND gift_detail.price > 1000
                -- gift không thuộc brand và cat nào vẫn đổi qùa bình thường
                LEFT JOIN urbox.brand ON gift_detail.brand_id = brand.id and brand.status = 2
                LEFT JOIN urbox.category ON brand.cat_id = category."id"
            WHERE
                issue_id = 8619
                AND card_holistic.status IN ( 1, 2, 5 )
                AND card_holistic.money > gift_detail.price
			GROUP BY gift_detail_id
			ORDER BY COUNT(*) DESC
        )
        SELECT 
            cte.*,
            gift_detail.title 
        FROM cte
            JOIN urbox.gift_detail ON gift_detail.id = cte.gift_detail_id
        ORDER BY cte.count_num DESC
    """  # noqa
    df = pd.read_sql_query(sql=raw_query_1, con=urbox_engine)
    total_trans = df['count_num'].sum()
    df['gift_rate'] = df['count_num'] / total_trans * 100
    df.sort_values(by=['gift_rate'], ascending=False).reset_index(drop=True)
    df = df.head(500)
    # print(df)
    GSheetApi.update_value(list_result=df.values.tolist(), grid_range_to_update='Account sheet!B10',
                           gsheet_id=gsheet_id)

    raw_query_2 = """
            with cte as (
                SELECT 
    				gift_detail_id,
    				COUNT(*) AS count_num								
                FROM ub_holistics.card_holistic
                    JOIN urbox.gift_set ON gift_set.gift_id = card_holistic.gift_set_id AND gift_set.status = 2
                    JOIN urbox.gift ON gift.id = gift_set.gift_id AND gift.status = 1
                    JOIN urbox.gift_detail ON gift_set.gift_detail_id = gift_detail.id AND gift_detail.status = 1 AND gift_detail.price > 1000
                    -- gift không thuộc brand và cat nào vẫn đổi qùa bình thường
                    LEFT JOIN urbox.brand ON gift_detail.brand_id = brand.id and brand.status = 2
                    LEFT JOIN urbox.category ON brand.cat_id = category."id"
                WHERE
                    card_holistic.status IN ( 1, 2, 5 )
                    AND card_holistic.money > gift_detail.price
    			GROUP BY gift_detail_id
    			ORDER BY COUNT(*) DESC
            )
            SELECT 
                cte.*,
                gift_detail.title 
            FROM cte
                JOIN urbox.gift_detail ON gift_detail.id = cte.gift_detail_id
            ORDER BY cte.count_num DESC
        """  # noqa
    df = pd.read_sql_query(sql=raw_query_2, con=urbox_engine)
    total_trans = df['count_num'].sum()
    df['gift_rate'] = df['count_num'] / total_trans * 100
    df.sort_values(by=['gift_rate'], ascending=False).reset_index(drop=True)
    df = df.head(500)
    print(df.head(10))
    GSheetApi.update_value(list_result=df.values.tolist(), grid_range_to_update='Account sheet!F10',
                           gsheet_id=gsheet_id)
    return df


if __name__ == "__main__":
    import time

    start_time = time.time()
    get_view()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
