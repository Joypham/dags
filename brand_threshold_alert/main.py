# from oauth2client.service_account import ServiceAccountCredentials
# from brand_threshold_alert.param import *
#
from Config import *
from Utility import Utility

import gspread
import psycopg2

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)

google_cloud = gspread.service_account(filename=GOOGLE_PRIVATE_KEY)
google_spread = google_cloud.open("Cảnh báo doanh thu đạt ngưỡng")


def main():
    list_brand = get_brand_config()
    list_payment = get_payment()
    print(list_brand)
    print(list_payment)
    for id, brand in list_brand.items():
        print(f"Kiểm tra dữ liệu brand: {id}")
        get_revenue_by_brand_id(id)

#     for brand_id in brand_list:
#         print('BRAND ID:', brand_id)
#         paid_amount, lastest_payment_date = get_payment_from_ggsheet(GOOGLE_SHEET_SCOPE, PO_APP_GOOGLE_SHEET_CREDENTIALS,
#                                                                      SPREADSHEET, PAYMENT_INFO_SHEET, brand_id)
#         lastest_time, lastest_level = get_log_from_ggsheet(GOOGLE_SHEET_SCOPE, PO_APP_GOOGLE_SHEET_CREDENTIALS, SPREADSHEET,
#                                                            SEND_LOG, brand_id)
#         with urbox_engine.connect() as conn:
#             try:
#                 result = conn.execute(sql_string.format(brand_id=brand_id)).first()
#                 brand_name = result[0]
#                 total_recorded_revenue = result[1]
#             except (TypeError):
#                 brand_name = ''
#                 total_recorded_revenue = 0
#         print('BRAND NAME:', brand_name)
#         threshold_dict = get_threshold_from_ggsheet(GOOGLE_SHEET_SCOPE, PO_APP_GOOGLE_SHEET_CREDENTIALS, SPREADSHEET,
#                                                     CONFIG_SHEET, brand_id)
#         print('THRESHOLD_DICT:', threshold_dict)
#         total_unrecorded = get_unrecoded_code_from_ggsheet(GOOGLE_SHEET_SCOPE, PO_APP_GOOGLE_SHEET_CREDENTIALS, SPREADSHEET,
#                                                            UNRECODED_CODE, brand_id)
#         print(f'TỔNG REVENUE TRÊN HỆ THỐNG LÀ {total_recorded_revenue:,}')
#         print(f'TỔNG REVENUE KHÔNG TRÊN HỆ THỐNG LÀ {total_unrecorded:,}')
#         print(f'TỔNG ĐÃ TRẢ LÀ {paid_amount:,}')
#         remaining_revenue = (total_recorded_revenue + total_unrecorded) - paid_amount
#         if remaining_revenue < 0:
#             print(f'ĐÃ TRẢ TRƯỚC {abs(remaining_revenue):,} CHO {brand_name}')
#             pass
#         else:
#             send, threshold_level, threshold_value = revenue_compare(threshold_dict, remaining_revenue)
#             duration = dt.datetime.now() - lastest_time.replace(tzinfo=None)
#             hours = duration.days * 24 + duration.seconds // 3600
#             if threshold_level == lastest_level and hours < 24:
#                 print('CHƯA CÓ THAY ĐỔI')
#                 pass
#             else:
#                 print(f'MỐC {threshold_level:,} VỚI DOANH THU CHƯA ĐƯỢC THANH TOÁN LÀ {remaining_revenue:,}')
#                 if send == True:
#                     subject = SUBJECT.format(brand_name=brand_name)
#                     text = threshold_text_dict[threshold_level].format(brand_name=brand_name,
#                                                                        threshold_value=f'{threshold_value:,}')
#                     send_notification_revenue_alert(brand_id, threshold_level, subject, text, send)
#                     max_index = get_index_from_ggsheet(GOOGLE_SHEET_SCOPE, PO_APP_GOOGLE_SHEET_CREDENTIALS, SPREADSHEET,
#                                                        SEND_LOG)
#                     df = pd.DataFrame({'brand_id': [brand_id], 'threshold_level': [threshold_level],
#                                        'lastest_time': [str(dt.datetime.now(tz=timezone('Asia/Jakarta')))], 'status': [1]})
#                     insert_data_to_gg_sheet2(PO_APP_GOOGLE_SHEET_CREDENTIALS, SPREADSHEET, SEND_LOG, df, max_index)
#                 else:
#                     print('KHÔNG VƯỢT MỐC')
#                     pass
#

def get_brand_config():
    config_sheet = google_spread.worksheet("config")
    config_data = config_sheet.get_all_records()
    list_brand = {}
    for config in config_data:
        if config.get("status") != 1:
            continue
        key = f"{config.get('brand_id')}"
        if key not in list_brand:
            list_brand.update({key: {}})
        list_brand.get(key).update({
            f"level_{config.get('threshold_level')}": Utility.to_int(config.get('value'))
        })
    return list_brand


def get_payment():
    payment_sheet = google_spread.worksheet("payment")
    payment_data = payment_sheet.get_all_records()
    list_payment = {}
    for payment in payment_data:
        key = f"{payment.get('brand_id')}"
        if key not in list_payment:
            list_payment.update({
                key: {
                    "payment_amount": 0,
                    "latest_payment_date": 0
                }
            })
        latest_payment_time = Utility.date_string_to_timestamp(payment.get("payment_date"))
        if latest_payment_time is False:
            continue
        if list_payment.get(key).get("latest_payment_date") > latest_payment_time:
            latest_payment_time = list_payment.get(key).get("latest_payment_date")
        payment_amount = list_payment.get(key).get("payment_amount") + payment.get("payment_amount")
        list_payment.get(key).update({
            "payment_amount": payment_amount,
            "latest_payment_date": latest_payment_time
        })
    return list_payment


def get_revenue_by_brand_id(brand_id):
    redshift_cursor = redshift_connection.cursor()
    redshift_cursor.execute(f"""
        SELECT b.title, SUM(cd.money) AS revenue
        FROM urbox.cart_detail cd
            LEFT JOIN urbox.cart c ON cd.cart_id = c.id
            LEFT JOIN (
                SELECT *, row_number() over(PARTITION BY cart_detail_id ORDER BY id DESC) rn_cd_id FROM urbox.gift_code
            ) gc ON gc.cart_detail_id = cd.id AND gc.rn_cd_id = 1 AND gc.status = 1
            LEFT JOIN urbox.brand b ON b.id = cd.brand_id
        WHERE
            cd.status = 2
            AND cd.pay_status = 2
            AND c.delivery <> 4
            AND gc.used > 0
            AND gc.used IS NOT NULL
            AND cd.brand_id = {brand_id}
        GROUP BY b.id
    """)
    result = redshift_cursor.fetchone()
    print(result)
# def get_brand_list_from_ggsheet(google_sheet_scope, PO_APP_GOOGLE_SHEET_CREDENTIALS, spreadsheet, sheetname, real='y'):
#     '''
#     real = 'y' => real, 'n' => test
#     '''
#     client_data = get_data_from_ggsheet(google_sheet_scope, PO_APP_GOOGLE_SHEET_CREDENTIALS, spreadsheet, sheetname)
#     try:
#         real_dct = {'y': 1, 'n': 0}
#         client_data = client_data[(client_data['status'] == 1) & (client_data['real'] == real_dct[real])]
#         client_data['brand_id'] = client_data['brand_id'].replace('', np.nan)
#         client_data = client_data.dropna(subset=['brand_id'])
#         return client_data['brand_id'].unique().tolist()
#     except Exception as e:
#         raise e
#
#
# def get_data_from_ggsheet(google_sheet_scope, PO_APP_GOOGLE_SHEET_CREDENTIALS, spreadsheet, sheetname):
#     client = conn_to_google_sheet(google_sheet_scope, PO_APP_GOOGLE_SHEET_CREDENTIALS)
#     sheet = client.open_by_key(spreadsheet).worksheet(sheetname)
#     client_data = sheet.get_all_records()
#     if not client_data:
#         client_data = pd.DataFrame(columns=SHEET_COL_DICT[sheetname])
#     else:
#         client_data = pd.DataFrame.from_dict(client_data)
#     return client_data
#
#
# def conn_to_google_sheet(google_sheet_scope, googlesheet_privatekey):
#     google_sheet_scope = google_sheet_scope
#     # add credentials to the account
#     creds = ServiceAccountCredentials.from_json_keyfile_name(googlesheet_privatekey, google_sheet_scope)
#     # authorize the clientsheet
#     client = gspread.authorize(creds)
#     return client
