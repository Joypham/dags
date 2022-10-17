gsheet_id = "1A1EMjx6TNwuHfA08IL8F5QXXknzvAYG_mz1fjgrQ640"
sheet_name_merchant_info = "Sheet1"

merchant_info_dict_column_name = {
    "brand_id": "brand_id",
    "cong_ty": "supplier_name",
    "brand": "brand_name",
    "project": "project",
    "chiet_khau": "discount",
    "thoi_han_hop_tac": "term",
    "hinh_thuc_van_hanh": "connect",
    "link_hop_dong": "link_contract",
    "link_plhd": "link_appendix",
    "ghi_chu": "note"
}

merchant_info_column_name = ', '.join(list(merchant_info_dict_column_name.values()))
QUERY_TO_UB_RAWDATA_URBOX_PO = f"INSERT into ub_rawdata.urbox_po ({merchant_info_column_name}) SELECT {merchant_info_column_name} from sentry.urbox_po"
