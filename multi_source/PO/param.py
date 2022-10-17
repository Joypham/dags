gsheet_id = "1txmP16JBsxxLqqHBvRnV2ZcgGT5JcH046pjRNjgTaIE"
sheet_name_client_sub_name = "client_sub_name"
sheet_name_sub_po = "Sub PO"
sheet_name_po = "PO"
sub_po_dict_column_name = {
    "po": "po_code",
    "client": "client_code",
    "loai": "po_type",
    "po/_contract": "po_contract",
    "po_tren_dot_cap_phat": "po_code_internal",
    "ma_po_doi_tac": "po_code_external",
    "po/_contract_value": "volume",
    "chiet_khau": "discount",
    "gia_tri_sau_chiet_khau": "volume_after_discount",
    "ngay_ban_giao": "trasaction_date",
    "so_tien_ban_giao_&_nghiem_thu": "handover_money",
    "account_doi_tac": "account_client",
    "so_ctt/_hoa_don": "po_description",
    "payment_status": "payment_status",
    "payment_status1": "payment_status_1",
    "acc_phu_trach": "account_member",
    "ghi_chu": "note",
    "thoi_gian_cap_phat": "export_time",
    "so_tien_thuc_te_tren_he_thong": "money_in_system",
    "ma_cap_phat_tren_he_thong": "po_name_system_2",
    "ten_po_tren_holistic": "po_name_report"
}
po_dict_column_name = {
    "client": "client",
    "app_name": "app_name",
    "app_id": "app_id",
    "client_subname": "client_subname",
    "thoi_gian_hieu_luc\n(mm/dd/yyyy)": "effective_date",
    "po_tong_id": "id",
    "phuong_thuc_ban_giao": "handover_type",
    "gia_tri_theo_hop_dong": "volume_in_contract",
    "gia_tri_thuc_te": "volume_real",
    "gia_tri_con_lai": "volume",
    "po_khach_hang_-\nlink_po_drive": "po_name",
    "note_1": "note1",
    "note_2": "note2",
    "a": "a",
}
po_column_name = ', '.join(list(po_dict_column_name.values()))
QUERY_TO_UB_RAWDATA_URBOX_PO = f"INSERT into ub_rawdata.urbox_po ({po_column_name}) SELECT {po_column_name} from sentry.urbox_po"
sub_po_column_name = ', '.join(list(sub_po_dict_column_name.values()))
QUERY_TO_UB_RAWDATA_URBOX_SUBPO = f"INSERT into ub_rawdata.urbox_subpo ({sub_po_column_name}) SELECT {sub_po_column_name} from sentry.urbox_subpo2"

client_sub_name_dict_column_name = {
    "client": "client_name",
    "client_sub_name": "client_code",
    "id": "id",
    "industry": "industry"
}
client_sub_name_column_name = ', '.join(list(client_sub_name_dict_column_name.values()))
QUERY_TO_UB_RAWDATA_URBOX_CLIENT_SUB_NAME = f"INSERT into ub_rawdata.tbl_client ({client_sub_name_column_name}) SELECT {client_sub_name_column_name} from sentry.tbl_client"