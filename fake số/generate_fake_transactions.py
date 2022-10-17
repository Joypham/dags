from core.redshift.urbox_redshift_connector.sql_alchemy_engine.URI import SQLALCHEMY_DATABASE_URI
from core.gsheet_api.function_new import GSheetApi
from core.gsheet_api import gsheet_cridential_path_user_urbox, gsheet_token_path_user_urbox
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import pandas as pd
from ortools.sat.python import cp_model
import numpy as np
import uuid

pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
# https://docs.google.com/spreadsheets/d/1_hEkr5VwM-LVBHQTySyyl1_l8qDxcYECrB0f4Aj5DqU/edit#gid=1661669589
gsheet_id = '1_hEkr5VwM-LVBHQTySyyl1_l8qDxcYECrB0f4Aj5DqU'
sheet_name = 'data_final test'
GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)

urbox_engine = create_engine(SQLALCHEMY_DATABASE_URI(schema_name="urbox"))
db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=urbox_engine))


class FakeTransaction:
    def __init__(self, variables, limit):
        self.batch_uuid = uuid.uuid4()
        self.created_date = time.date()

def get_constrain():
    # fake 50% money_remain
    raw_query_1 = """
        SELECT 
        issue_id,
        SUM(money_topup) as money_topup,
        SUM(money) as money_remain
        from ub_holistics.card_holistic
        WHERE
        issue_id in (8623,8619, 9791)
        AND
        status IN ( 1, 2, 5 )
        GROUP BY
        issue_id
    """
    df = pd.read_sql_query(sql=raw_query_1, con=urbox_engine)
    df['money_fake'] = df['money_remain'] / 2
    df['money_fake'] = df['money_fake'].astype('int64')
    print(df)
    return df


def get_data():
    raw_query_1 = """
        SELECT 
			card_holistic.issue_id,
			card_holistic.app_id,
			card_holistic.number as card_number,
			card_holistic.phone_sub,						
			card_holistic.card_id,						
			card_holistic.money as money_remain,
			card_holistic.money_topup as money_topup,
			gift_detail.id as gift_detail_id,
			gift_detail.title,
			gift_detail.price as money_gift,
			gift.id as gift_id,
			brand."id" as brand_id,
 			brand.title as brand_title,
 			category.title as cat_title												
        FROM ub_holistics.card_holistic
            JOIN urbox.gift_set on gift_set.gift_id = card_holistic.gift_set_id and gift_set.status = 2
            JOIN urbox.gift on gift.id = gift_set.gift_id and gift.status = 1
            JOIN urbox.gift_detail on gift_set.gift_detail_id = gift_detail.id AND gift_detail.status = 1 and gift_detail.price > 1000
            -- gift không thuộc brand và cat nào vẫn đổi qùa bình thường
			LEFT JOIN urbox.brand on gift_detail.brand_id = brand.id and brand.status = 2
 			LEFT JOIN urbox.category on brand.cat_id = category."id"
        WHERE
            -- issue_id in (8623,8619,9791)
            issue_id = 8619
            AND card_holistic.status IN ( 1, 2, 5 )
            AND card_holistic.money > 10000
            AND card_holistic.money > gift_detail.price
        ORDER BY 
            card_id, gift_detail.id
    """  # noqa
    df = pd.read_sql_query(sql=raw_query_1, con=urbox_engine)
    # 1 quà mua tối đa 10 lần
    df['range'] = df.apply(
        lambda x: 3 if round(int(x['money_remain']) / int(x['money_gift'])) > 3 else round(
            int(x['money_remain']) / int(x['money_gift'])), axis=1).reset_index(drop=True)
    df['x_variable'] = "x" + df.index.astype('str')
    return df


def create_data_model():
    df = get_data().copy()
    # df.to_csv("/Users/phamhanh/Downloads/fake_so_model.csv", sep=',', encoding='utf-8')
    # GSheetApi.delete_sheet(gsheet_id=gsheet_id, sheet_name='data')
    # GSheetApi.creat_new_sheet_and_update_data_from_df(df=df, gsheet_id=gsheet_id, new_sheet_name="data")
    cards = list(set(df['card_id'].tolist()))
    df_bound_coeff_1 = df[['card_id', 'money_remain']].drop_duplicates().reset_index(drop=True)
    data = {}
    bound_coeff_1 = []

    constraint_coeffs = []
    for card in cards:
        df[card] = df.apply(lambda x: x['money_gift'] if x['card_id'] == card else 0, axis=1)
        k = int(df_bound_coeff_1[df_bound_coeff_1['card_id'] == card]['money_remain'])
        bound_coeff_1.append(k)
    df_constraint_coeff_1 = df[cards]

    # số tiền tiêu <= số tiền còn lại trong thẻ
    constraint_coeff_1 = np.transpose(df_constraint_coeff_1.to_numpy())

    # Tổng tiền tiêu nhỏ hơn cận trên PO: x1*y1 + x2*y2 ... <= cận trên
    constraint_coeff_2 = df['money_gift'].tolist()
    # print(constraint_coeff_2)

    # Tổng tiền tiêu lớn hơn cận dưới PO: -1 * (x1*y1 + x2*y2 ...) <= - cận dưới
    df['money_gift'] = df['money_gift'] * (-1)
    constraint_coeff_3 = df['money_gift'].tolist()

    '''
            phương trình tổng quát về tỷ lệ sử dụng quà được transform như sau:
            tỷ lệ sử dụng quà n: Xn/ X1+ X2+...+Xn >= k/ 100
            <=> 100*Xn >= kX1 + kX2 + kX3 + ....+ kXn
            <=> kX1 + kX2 + ...+ (k-100)Xn <= 0
            VD: tỷ lệ sử dụng quà 0 >= 10%
            (10-100)X0 + 10X1+ 10X2+ ....+ Xn <= 0
    '''
    # tỷ lệ sử dụng quà gift_detail_id_845 >= 10%
    df['gift_rate'] = df.apply(lambda x: (10 - 100) if x['gift_detail_id'] == 845 else 10, axis=1)
    constraint_coeff_4 = df['gift_rate'].tolist()

    for constraint_coeff in constraint_coeff_1.tolist():
        constraint_coeffs.append(constraint_coeff)
    constraint_coeffs.append(constraint_coeff_2)
    constraint_coeffs.append(constraint_coeff_3)
    constraint_coeffs.append(constraint_coeff_4)

    data['constraint_coeffs'] = constraint_coeffs

    # lấy dữ liệu constrain
    get_constrain()

    # [cận trên, - cận dưới, 0]
    data['bounds'] = bound_coeff_1 + [10000000, (-9000000), 0]
    data['range'] = df['range'].tolist()
    data['num_vars'] = df.index.stop
    data['num_constraints'] = len(constraint_coeffs)

    return data


def get_solver(num_of_solution: int = 1):
    data = create_data_model()
    # 2.2 create model and declare variable
    model = cp_model.CpModel()
    list_var = []
    x = {}
    for j in range(data['num_vars']):
        x[j] = model.NewIntVar(0, data['range'][j], 'x[%i]' % j)
        list_var.append(x[j])

    # Add constraint
    for i in range(data['num_constraints']):
        constraint_expr_1 = [int(data['constraint_coeffs'][i][j]) * x[j] for j in range(data['num_vars'])]
        model.Add(sum(constraint_expr_1) <= data['bounds'][i])
    #     if i == 2:
    #         # tổng money consuming của 1 PO lớn hơn cận dưới
    #         model.Add(sum(constraint_expr_1) >= data['bounds'][i])
    #     else:
    #         # tổng money consuming của 1 thẻ nhỏ hơn số tiền còn lại trong thẻ
    #         # tổng money consuming của 1 PO nhỏ hơn cận trên
    #         # (10-100)X0 + 10X1+ 10X2+ ....+ Xn <= 0

    # # Only get 1 result
    solver = cp_model.CpSolver()
    solution_printer = VarArraySolutionPrinterWithLimit(list_var, num_of_solution)
    solver.parameters.enumerate_all_solutions = True
    status = solver.Solve(model, solution_printer)
    results = []
    for i in list_var:
        results.append(solver.Value(i))
    return results


if __name__ == "__main__":
    import time

    start_time = time.time()

    # get data
    # 1.1 view constrain
    get_constrain()

    # 1.2 view data
    df = get_data()

    # 2.1 create model and declare variable

    df['result'] = get_solver()
    final_df = df[df['result'] != 0].fillna('None')
    GSheetApi.delete_sheet(gsheet_id=gsheet_id, sheet_name='fake_so_results')
    GSheetApi.creat_new_sheet_and_update_data_from_df(df=final_df, gsheet_id=gsheet_id,
                                                      new_sheet_name="fake_so_results")
    # final_df.to_csv("/Users/phamhanh/Downloads/fake_so_results.csv", sep=',', encoding='utf-8')

    # GSheetApi.update_value_at_last_column(df_to_update=df[['result']], gsheet_id=gsheet_id, sheet_name='data',
    #                                       start_row=1)

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
