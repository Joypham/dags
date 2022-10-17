gsheet_id = "1yXUIGE_PmyH-fauI0xQkOlAcgRMn0UgjGq8GKTpUGJA"
sheet_name_ps = "Sheet1"
ps_dict_column_name = {
    "stt": "stt",
    "brand_id": "brand_id",
    "dich_vu": "service",
    "codeub": "code_ub",
    "code": "code",
    "ngay_tiep_nhan_yeu_cau": "ticket_received_time",
    "tinh_trang_su_dung": "using_status",
    "ngay_su_dung": "using_date",
}
ps_column_name = ', '.join(list(ps_dict_column_name.values()))
QUERY_TO_UB_RAWDATA_URBOX_PS = f"INSERT into ub_rawdata.cs_premium ({ps_column_name}) SELECT {ps_column_name} from sentry.cs_premium"