# from oauth2client.service_account import ServiceAccountCredentials
# from brand_threshold_alert.param import *
#
from datetime import datetime
from Config import *
from Utility import Utility

import gspread
import psycopg2
import psycopg2.extras

redshift_connection = psycopg2.connect(**REDSHIFT_CONFIG)

google_cloud = gspread.service_account(filename=GOOGLE_PRIVATE_KEY)
google_spread = google_cloud.open("Cảnh báo doanh thu đạt ngưỡng")


def main():
    create_log(123, 9)
    return
    current = Utility.current_timestamp()
    list_brand = get_brand_config()
    list_payment = get_payment()
    list_unrecorded_revenue = get_unrecorded_revenue()
    list_log = get_log()
    print(list_brand)
    print(list_payment)
    print(list_unrecorded_revenue)
    for id, threshold in list_brand.items():
        print(f"Kiểm tra dữ liệu brand: {id}")
        revenue = get_revenue_by_brand_id(id)
        unrecorded_revenue = list_unrecorded_revenue.get(f"{id}") or 0
        payment_detail = list_payment.get(f"{id}") or None
        if payment_detail is not None:
            paid_amount = payment_detail.get("payment_amount")
            latest_payment_date = payment_detail.get("latest_payment_date")
        else:
            paid_amount = 0
            latest_payment_date = 0

        log = list_log.get(f"{id}") or None
        if log is not None:
            latest_level = log.get("level")
            latest_log = log.get("latest_log")
        else:
            latest_level = 0
            latest_log = 0

        remaining_revenue = revenue + unrecorded_revenue - paid_amount
        print(f"Thông tin brand: {revenue.get('title')}")
        print(f"Tổng doanh thu đã ghi nhận trên hệ thống là {revenue.get('revenue')}")
        print(f"Tổng doanh thu chưa ghi nhận trên hệ thống là {unrecorded_revenue}")
        print(f"Tổng số tiền đã thanh toán là {paid_amount}")
        print(f"Số tiền chưa thanh toán là {remaining_revenue}")
        if remaining_revenue <= 0:
            print("Đã thanh toán tất cả")
            continue

        warning_level = check_threshold(threshold, remaining_revenue)
        if warning_level is False:
            print("Số tiền chưa thanh toán không chạm mốc cảnh báo.")
            continue

        diff_latest_log_hours = (current - latest_log) // 3600
        if latest_level == warning_level and diff_latest_log_hours < 24:
            print("Brand này đã được cảnh báo trong 24h vừa qua")
            continue
        print(f"""
            Brand {revenue.get('title')} đã chạm tới mốc {warning_level} 
            với doanh thu chưa được thanh toán là {remaining_revenue}
        """)
        # Gửi mail
        # Lưu log
        insert_log(id, warning_level)
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
            f"{config.get('threshold_level')}": Utility.to_int(config.get('value'))
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


def get_unrecorded_revenue():
    unrecorded_code_sheet = google_spread.worksheet("unrecorded_code")
    unrecorded_code_data = unrecorded_code_sheet.get_all_records()
    list_unrecorded_revenue = {}
    for code in unrecorded_code_data:
        key = f"{code.get('brand_id')}"
        if key not in list_unrecorded_revenue:
            list_unrecorded_revenue.update({
                key: 0
            })
        list_unrecorded_revenue.update({
            key: list_unrecorded_revenue.get(key) + code.get("value")
        })
    return list_unrecorded_revenue


def get_log():
    log_sheet = google_spread.worksheet("send_log")
    log_data = log_sheet.get_all_records()
    list_log = {}
    for log in log_data:
        key = f"{log.get('brand_id')}"
        if key not in list_log:
            list_log.update({
                key: {
                    "level": 0,
                    "latest_log": 0
                }
            })
        latest_log = Utility.date_string_to_timestamp(log.get("log_time"))
        if list_log.get(key).get("latest_log") >= latest_log:
            continue
        list_log.get(key).update({
            "level": log.get("threshold_level"),
            "latest_log": latest_log
        })
    return list_log


def create_log(brand_id, threshold_level):
    log_sheet = google_spread.worksheet("send_log")
    log_sheet.append_row({brand_id, threshold_level, str(datetime.today())})


def get_revenue_by_brand_id(brand_id):
    redshift_cursor = redshift_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
        GROUP BY b.title
    """)
    result = redshift_cursor.fetchone()
    redshift_cursor.close()
    return result


def check_threshold(threshold, revenue):
    for level, value in threshold.items():
        if revenue >= value:
            return level
    return False
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
