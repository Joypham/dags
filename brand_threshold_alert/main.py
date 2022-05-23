# from oauth2client.service_account import ServiceAccountCredentials
# from brand_threshold_alert.param import *
#
from Config import *

import gspread


# import pandas as pd

def main():
    google_cloud = gspread.service_account(filename=GOOGLE_PRIVATE_KEY)
    google_spread = google_cloud.open("Cảnh báo doanh thu đạt ngưỡng")
    print(google_spread)
    pass
#     brand_list = get_brand_list_from_ggsheet(GOOGLE_SHEET_SCOPE, PO_APP_GOOGLE_SHEET_CREDENTIALS, SPREADSHEET, CONFIG_SHEET)
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
#
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
