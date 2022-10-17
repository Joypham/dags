datamart_card_issue_list_columns = ["po_number",
                                    "card_number",
                                    "card_id",
                                    "phone",
                                    "customer_name",
                                    "money_remain",
                                    "chuyen_tien_sang_the_khac",
                                    "hoan_tien",
                                    "money_comsuming",
                                    "money_topup",
                                    "nhan_tien_tu_the_khac",
                                    "type_7",
                                    "type_8"
                                    ]
card_issue_list_columns = ', '.join(datamart_card_issue_list_columns)

QUERY_CARD_ISSUE_LIST_SYNC = f"INSERT INTO ub_rawdata.datamart_card_issue_list ({card_issue_list_columns}) SELECT {card_issue_list_columns} from sentry.datamart_card_issue_list;"
print(QUERY_CARD_ISSUE_LIST_SYNC)


datamart_chi_tiet_doi_qua_columns = ["customer_name", "phone", "card_id", "card_number", "cart_id", "money_ship", "money_gift", "money_fee", "hoan_tien", "money_comsuming", "cart_detail_id", "gift_id", "category_title", "brand_title", "quantity", "cart_detail_money"]
chi_tiet_doi_qua_columns = ', '.join(datamart_chi_tiet_doi_qua_columns)

QUERY_CHI_TIET_DOI_QUA_SYNC = f"INSERT INTO ub_rawdata.datamart_chi_tiet_doi_qua ({chi_tiet_doi_qua_columns}) SELECT {chi_tiet_doi_qua_columns} from sentry.datamart_chi_tiet_doi_qua;"
print(QUERY_CHI_TIET_DOI_QUA_SYNC)