SHEET_COL_DICT = {
    'revenue_test': ['brand_id', 'revenue'],
    'config': ['brand_id', 'threshold_level', 'value', 'status', 'real'],
    'brand_info_test': ['brand_id', 'brand_name'],
    'payment': ['brand_id', 'payment_date', 'payment_amount', 'doc'],
    'unrecorded_code': ['brand_id', 'using_time', 'gift_detail_name', 'code', 'value'],
    'send_log': ['brand_id', 'threshold_level', 'lastest_time', 'status'],
    'mail_list_test': ['brand_id', 'internal_mail_list', 'external_mail_list'],
}
GOOGLE_FEEDS = 'https://spreadsheets.google.com/feeds'
GOOGLE_DRIVE = 'https://www.googleapis.com/auth/drive'
GOOGLE_SHEET_SCOPE = [GOOGLE_FEEDS, GOOGLE_DRIVE]
