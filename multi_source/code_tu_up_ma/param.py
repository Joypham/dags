gsheet_id = "1CC481I2HkluF8FOv137P-oh38de3RHbspGjbzM18h78"
sheet_name_code_tu_up_ma = "Sheet1"
get_cart_detail_by_hash_code = """
    SELECT 
        a.code,
        a.cart_detail_id
    FROM urbox.gift_code a
        LEFT JOIN urbox.cart_detail b ON a.cart_detail_id = b.id
    WHERE 
        1 = 1
        AND b.id IS NOT NULL 
        AND b.status = 2
        AND b.pay_status = 2
        AND a.code IN {hash_code}
"""
code_tu_up_ma_column_name = '"id", "brand", "brand_id", "gift_detail_title", "brand_code", "using_time", "code_decryption", "cart_detail_id", "gift_detail_price"'
k = code_tu_up_ma_column_name.replace('"using_time"', 'CAST("using_time" as date)')
QUERY_TO_UB_RAWDATA_URBOX_CODE_TU_AP_MA = f"INSERT into ub_rawdata.code_tu_up_ma ({code_tu_up_ma_column_name}) SELECT {k} from sentry.code_tu_up_ma"
