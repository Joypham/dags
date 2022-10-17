from file import gsheet_token_path_user_urbox, gsheet_cridential_path_user_urbox
from multi_source.fuction import GSheetApi
from sqlalchemy import create_engine
import pandas as pd
import unidecode
import codecs
from sqlalchemy.orm import scoped_session, sessionmaker
from Config import *
from aia_report.param import *
import time
import re

# https://docs.google.com/spreadsheets/d/1bEBhwguN9QUWkd5WwOpc4_QXe-SkdD_FVUsWimlja4A/edit#gid=1430690001&fvid=211793291
# gsheet_id = '1bEBhwguN9QUWkd5WwOpc4_QXe-SkdD_FVUsWimlja4A'
# sheet_name = 'Sheet1'
REDSHIFT_URI = f"postgresql+pg8000://{REDSHIFT_CONFIG['user']}:{REDSHIFT_CONFIG['password']}@{REDSHIFT_CONFIG['host']}:{REDSHIFT_CONFIG['port']}/{REDSHIFT_CONFIG['dbname']}"
SENTRY_URI = f"mysql+mysqldb://{SENTRY_CONFIG['user']}:{SENTRY_CONFIG['password']}@{SENTRY_CONFIG['host']}:{SENTRY_CONFIG['port']}/{SENTRY_CONFIG['dbname']}"

GSheetApi = GSheetApi(credentials_path=gsheet_cridential_path_user_urbox, token_path=gsheet_token_path_user_urbox)
urbox_conection = create_engine(REDSHIFT_URI)
redshift_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=urbox_conection))
sentry_connection = create_engine(SENTRY_URI)
sentry_db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=sentry_connection))

ESCAPE_SEQUENCE_RE = re.compile(r"""
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )""", re.UNICODE | re.VERBOSE)


def de_emojify(string):
    regex_pattern = re.compile(pattern="["
                                       u"\U0001F600-\U0001F64F"  # emoticons
                                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                       u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                       "]+", flags=re.UNICODE)
    return regex_pattern.sub(r'', string)


def decode_escapes(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')

    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)


def get_card_transaction_type_name(type: int):
    '''
        https://docs.google.com/spreadsheets/d/1-3Ya-edeLvoAavseLQF12fymwRBvcjva/edit#gid=1129270301&fvid=211587344
        Giao dich tru tien: 2, 4, 41, 404, 5, 7, 9, 11, 12
        Giao dich cong tien: 1, 3, 6, 8, 10
    '''

    if type == 1:
        name = 'Giao dịch Topup tiền vào'
    elif type == 2:
        name = 'Giao dịch tiêu dùng'
    elif type == 3:
        name = 'Refund'
    elif type == 4:
        name = 'Giao dịch trừ Allowance'
    elif type == 41:
        name = 'Giao dịch trừ do nợ allowance'
    elif type == 404:
        name = 'Reset Money'
    elif type == 5:
        name = 'Trừ tiền do chuyển sang APP'
    elif type == 6:
        name = 'Cộng tiền do nạp thẻ cào'
    elif type == 7:
        name = 'Trừ tiền do chuyển sang card khác'
    elif type == 8:
        name = 'Cộng tiền do nhận tiền từ card khác'
    elif type == 9:
        name = 'Trừ tiền do thu phí giao dịch chuyển tiền'
    elif type == 10:
        name = 'Cộng tiền do Allowance'
    elif type == 11:
        name = 'Trừ tiền do chuyển vào ví Urbox'
    elif type == 12:
        name = 'Trừ tiền do đối xoát'
    return name


def type_reformat(type: int):
    if type in (1, 3, 6, 8, 10):
        k = 1
    if type in (2, 4, 41, 404, 5, 7, 9, 11, 12):
        k = (-1)
    return k


def type_trasform(type: int):
    if type in (1, 404):
        return 'money_topup'
    elif type == 2:
        return 'money_comsuming'
    elif type == 7:
        return 'chuyen_tien_sang_the_khac'
    elif type in (4, 41, 404, 5, 9, 11, 12):
        return 'chuyen_tien_di'
    elif type == 8:
        return 'nhan_tien_tu_the_khac'
    else:
        return 'hoan_tien'


def main_aia_chi_tiet_doi_qua():
    # Step 2.1: get card_id in AIA
    raw_query_1 = "SELECT DISTINCT request_topup.po_number,card.number as card_number,card.id as card_id,customer.phone,customer.fullname as customer_name, card.money as money_remain\
                        FROM urbox.request_topup JOIN urbox.card ON card.ID = request_topup.card_id LEFT JOIN urbox.card_issue ON card_issue.id = card.issue_id\
                        LEFT JOIN urbox.customer  ON request_topup.customer_id = customer.id\
                        WHERE card.status IN ( 1, 2, 5 )\
                        AND card.TYPE NOT IN ( 2, 3 )\
                        AND customer.fullname not like '%test%'\
                        AND customer.fullname not like '%TEST%'"
    df_card = pd.read_sql_query(sql=raw_query_1, con=urbox_conection)
    card_id = df_card['card_id'].tolist()

    # Step 2.2: get card_trans in AIA
    raw_query_3 = f"SELECT 	card_transaction.card_id, card_transaction.type as card_trans_type, card_transaction.money as trans_money,\
                        cart.delivery, cart.id as cart_id, cart.money_ship, cart.money_gift, cart.money_fee\
                        FROM	urbox.card_transaction\
                        LEFT JOIN urbox.cart ON cart.id = card_transaction.cart_id and cart.status = 2\
                        WHERE card_transaction.status = 2 AND card_transaction.card_id in {tuple(card_id)}\
    	                ORDER BY card_transaction.card_id"
    card_trans = pd.read_sql_query(sql=raw_query_3, con=urbox_conection).fillna(0).astype(int)
    df_card_trans = df_card.merge(card_trans, how='left', on='card_id', sort='card_id').fillna(0)
    df_card_trans['customer_name'] = df_card_trans['customer_name'].astype('str')
    df_card_trans['customer_name'] = df_card_trans['customer_name'].apply(lambda x: decode_escapes(x))

    df_card_trans['trans_money'] = df_card_trans.apply(lambda x: x['trans_money'] * type_reformat(x['card_trans_type']),
                                                       axis=1)
    df_card_trans['card_trans_type'] = df_card_trans.apply(lambda x: type_trasform(type=x['card_trans_type']),
                                                           axis=1)

    df_cart_trans_columns = ['customer_name', 'phone', 'card_id', 'card_number', 'cart_id', 'card_trans_type',
                             'trans_money',
                             'money_ship', 'money_gift', 'money_fee']
    df_cart_trans = df_card_trans[df_card_trans['cart_id'] != 0].reset_index(drop=True)
    df_cart_trans = df_cart_trans[df_cart_trans_columns].reset_index(drop=True)

    # Get card_trans money_consuming to check
    df_card_trans = df_card_trans.groupby(
        ['po_number', 'card_number', 'card_id', 'phone', 'customer_name', 'money_remain', 'card_trans_type']).agg(
        {'trans_money': 'sum'}).fillna(0).reset_index()
    df_card_trans = df_card_trans.pivot_table(
        index=['po_number', 'card_number', 'card_id', 'phone', 'customer_name', 'money_remain'],
        columns='card_trans_type', values='trans_money', aggfunc='first').fillna(
        0).reset_index()

    # Step 2: get cart_cart_trans in AIA
    df_cart_trans = df_cart_trans.pivot_table(
        index=['customer_name', 'phone', 'card_id', 'card_number', 'cart_id', 'money_ship', 'money_gift', 'money_fee'],
        columns='card_trans_type', values='trans_money',
        aggfunc='first').fillna(0).reset_index()
    cart_id = df_cart_trans['cart_id'].tolist()
    print(df_cart_trans['money_comsuming'].sum())
    # Step 4: get cart_detail

    raw_query_4 = f"SELECT 	cart_detail.cart_id,cart_detail.id as cart_detail_id , gift.id as gift_id, category.title as category_title, brand.title as brand_title, cart_detail.quantity,cart_detail.money as cart_detail_money\
                        from urbox.cart_detail\
                        LEFT JOIN urbox.gift on gift.id = cart_detail.gift_id\
                        LEFT JOIN urbox.category on category.id = gift.cat_id\
                        LEFT JOIN urbox.brand on brand.id = cart_detail.brand_id\
                        WHERE cart_id in {tuple(cart_id)}\
                        AND pay_status = 2 AND cart_detail.status = 2"
    df_cart_detail = pd.read_sql_query(sql=raw_query_4, con=urbox_conection)
    # # Step 5: merge_cart_detail
    df_merge_cart_detail = df_cart_trans.merge(df_cart_detail, how='left', on='cart_id')

    # Step 6: remove dup_money_ship_and_money_fee by cart (1 cart bi charge 1 lan phi ship)
    df_merge_cart_detail['rank_cart'] = df_merge_cart_detail.groupby('cart_id')['money_ship'].rank(
        method='first')
    df_merge_cart_detail['money_ship'] = df_merge_cart_detail.apply(
        lambda x: x['money_ship'] if x['rank_cart'] == 1 else 0, axis=1)
    df_merge_cart_detail['money_fee'] = df_merge_cart_detail.apply(
        lambda x: x['money_fee'] if x['rank_cart'] == 1 else 0, axis=1)

    # Step 7:checking ship+fee_gift = consuming

    ship = df_merge_cart_detail['money_ship'].sum()
    fee = df_merge_cart_detail['money_fee'].sum()
    gift = df_merge_cart_detail['cart_detail_money'].sum()
    # print(ship + fee + gift)
    df_merge_cart_detail.drop('rank_cart', axis=1, inplace=True)
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.datamart_chi_tiet_doi_qua")
    redshift_db_session.commit()
    sentry_db_session.execute("TRUNCATE TABLE sentry.datamart_chi_tiet_doi_qua")
    sentry_db_session.commit()
    for i in range(0, len(df_merge_cart_detail), 1000):
        batch_df = df_merge_cart_detail.loc[i:i + 999]
        batch_df.to_sql("datamart_chi_tiet_doi_qua", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        # print(batch_df.tail(3))
    time.sleep(120)
    redshift_db_session.execute(QUERY_CHI_TIET_DOI_QUA_SYNC)
    redshift_db_session.commit()


def main_aia_chi_tiet_the_phat_hanh():
    # Step 1: Chi tiet PO : saved query ub_rawdata
    # Step 2: Danh sach the phat hanh

    # Step 2.1: get card_id in AIA
    raw_query_1 = "SELECT DISTINCT request_topup.po_number,card.number as card_number,card.id as card_id,customer.phone,customer.fullname as customer_name, card.money as money_remain\
                        FROM urbox.request_topup JOIN urbox.card ON card.ID = request_topup.card_id LEFT JOIN urbox.card_issue ON card_issue.id = card.issue_id\
                        LEFT JOIN urbox.customer  ON request_topup.customer_id = customer.id\
                        WHERE card.status IN ( 1, 2, 5 )\
                        AND card.TYPE NOT IN ( 2, 3 )\
                        AND customer.fullname not like '%test%'\
                        AND customer.fullname not like '%TEST%'\
                        AND request_topup.po_number not like '%TEST%'"
    df_card = pd.read_sql_query(sql=raw_query_1, con=urbox_conection)
    card_id = df_card['card_id'].tolist()

    # Step 2.2: get card_trans in AIA
    raw_query_3 = f"SELECT 	card_transaction.card_id, case when (card_transaction.note LIKE'%Hoàn phí vận chuyển%' or card_transaction.note LIKE'%cộng bù phí VC%')  THEN 3  else card_transaction.TYPE end card_trans_type, card_transaction.money as trans_money,\
                        cart.delivery, cart.id as cart_id, cart.money_ship, cart.money_gift, cart.money_fee\
                        FROM	urbox.card_transaction\
                        LEFT JOIN urbox.cart ON cart.id = card_transaction.cart_id and cart.status = 2\
                        WHERE card_transaction.status = 2 AND card_transaction.card_id in {tuple(card_id)}\
    	                ORDER BY card_transaction.card_id"
    card_trans = pd.read_sql_query(sql=raw_query_3, con=urbox_conection).fillna(0)
    df_card_trans = df_card.merge(card_trans, how='left', on='card_id', sort='card_id').fillna(0)

    # convert customer name to unicode
    df_card_trans['customer_name'] = df_card_trans['customer_name'].astype('str')
    # df_card_trans['customer_name'] = df_card_trans['customer_name'].apply(
    #     lambda x: codecs.getdecoder('unicode_escape')(x)[0])
    # df_card_trans['customer_name'] = df_card_trans['customer_name'].apply(lambda x: unidecode.unidecode(x))
    df_card_trans['customer_name'] = df_card_trans['customer_name'].apply(lambda x: decode_escapes(x))

    # trans_form sign of trans_money theo type
    df_card_trans['trans_money'] = df_card_trans.apply(lambda x: x['trans_money'] * type_reformat(x['card_trans_type']),
                                                       axis=1)
    df_card_trans['card_trans_type'] = df_card_trans.apply(lambda x: type_trasform(type=x['card_trans_type']), axis=1)

    # Coppy to cart_trans
    df_cart_trans_columns = ['customer_name', 'phone', 'card_id', 'card_number', 'cart_id', 'card_trans_type',
                             'trans_money',
                             'money_ship', 'money_gift', 'money_fee']
    df_cart_trans = df_card_trans[df_card_trans['cart_id'] != 0].reset_index(drop=True)
    df_cart_trans = df_cart_trans[df_cart_trans_columns].reset_index(drop=True)

    # group by trans_type
    df_card_trans = df_card_trans.groupby(
        ['po_number', 'card_number', 'card_id', 'phone', 'customer_name', 'money_remain',
         'card_trans_type']).agg(
        {'trans_money': 'sum'}).fillna(0).reset_index()

    df_card_trans = df_card_trans.pivot_table(
        index=['po_number', 'card_number', 'card_id', 'phone', 'customer_name', 'money_remain'],
        columns='card_trans_type', values='trans_money', aggfunc='first').fillna(
        0).reset_index()

    df_cart_trans = df_cart_trans.pivot_table(
        index=['customer_name', 'phone', 'card_id', 'card_number', 'cart_id', 'money_ship', 'money_gift', 'money_fee'],
        columns='card_trans_type', values='trans_money',
        aggfunc='first').fillna(0).reset_index()

    # Step 3: get card_trans_type
    raw_query_4 = f"SELECT card_id, TYPE, split_part(note,':',2) from urbox.card_transaction WHERE (card_transaction.type = 7 OR card_transaction.type = 8) AND\
                        card_id in {tuple(card_id)} ORDER BY card_id"
    df_card_trans_type = pd.read_sql_query(sql=raw_query_4, con=urbox_conection)
    # df_card_trans_type['type'] = df_card_trans_type.apply(lambda x: type_trasform(type=int(x['type'])), axis=1)
    df_card_trans_type = df_card_trans_type.pivot_table(
        index=['card_id'],
        columns='type', values='split_part',
        aggfunc=lambda x: list(x)).fillna(0).reset_index()
    # Step 4: merge
    df_card_trans = df_card_trans.merge(df_card_trans_type, how='left', on='card_id', sort='card_id').fillna(0)
    df_card_trans = df_card_trans.rename({7: 'type_7', 8: 'type_8'}, axis=1)

    # Step 5: checking:
    print(df_card_trans['money_comsuming'].sum())

    # Step 2.3: to_data_mart
    # Step 2.3.1: set dtype()
    columns_type_int = ['chuyen_tien_sang_the_khac', 'hoan_tien', 'money_comsuming', 'money_topup',
                        'nhan_tien_tu_the_khac']
    df_card_trans[columns_type_int] = df_card_trans[columns_type_int].astype('int64')
    columns_type_str = ['po_number', 'card_number', 'card_id', 'phone', 'customer_name', 'type_7', 'type_8']
    df_card_trans[columns_type_str] = df_card_trans[columns_type_str].astype('str')
    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.datamart_card_issue_list")
    redshift_db_session.commit()
    sentry_db_session.execute("TRUNCATE TABLE sentry.datamart_card_issue_list")
    sentry_db_session.commit()
    for i in range(0, len(df_card_trans), 1000):
        batch_df = df_card_trans.loc[i:i + 999]
        batch_df.to_sql("datamart_card_issue_list", con=sentry_connection, if_exists='append', index=False,
                        schema='sentry')
        print(batch_df.tail(3))
    time.sleep(120)
    redshift_db_session.execute(QUERY_CARD_ISSUE_LIST_SYNC)
    redshift_db_session.commit()


def main_aia_danh_sach_po():
    # Step 1 get df_amount_co:
    # time.sleep(120)
    raw_query_2 = f"SELECT app.name as app_name, app.id as app_id,co_number as po_number,amount_co, amount_actual FROM urco.customer_order LEFT JOIN urbox.app ON app.ID = customer_order.app_id  WHERE lower(co_number) not like '%test%' and customer_order.status = 2"
    df_amount_co = pd.read_sql_query(sql=raw_query_2, con=urbox_conection)

    # Step 2: get actual PO topup:
    raw_query_4 = "SELECT po_number, SUM(money_topup) as money_topup from ub_rawdata.datamart_card_issue_list GROUP BY po_number"
    df_card_top_up = pd.read_sql_query(sql=raw_query_4, con=urbox_conection)

    # merge
    df_amount_co = df_amount_co.merge(df_card_top_up, how='left', on='po_number', sort='card_id').fillna(0)
    df_amount_co['money_remain'] = df_amount_co.apply(
        lambda x: x['amount_actual'] - x['money_topup'], axis=1)
    column_type_int = ['amount_co', 'amount_actual', 'money_topup', 'money_remain']
    df_amount_co[column_type_int] = df_amount_co[column_type_int].astype('int64')
    print(df_amount_co)

    redshift_db_session.execute("TRUNCATE TABLE ub_rawdata.danh_sach_po")
    redshift_db_session.commit()
    df_amount_co.to_sql("danh_sach_po", con=urbox_conection, if_exists='append', index=False,
                        schema='ub_rawdata')


if __name__ == "__main__":
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    start_time = time.time()
    main_aia_chi_tiet_doi_qua()
    main_aia_chi_tiet_the_phat_hanh()
    main_aia_danh_sach_po()

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))

