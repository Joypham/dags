gsheet_id = "1txmP16JBsxxLqqHBvRnV2ZcgGT5JcH046pjRNjgTaIE"
sheet_name_app_name_to_use = "app_name_to_use"

app_name_to_use_dict_column_name = {
    "id": "app_id",
    "title": "app_name",
    "name": "short_name",
    "categorize": "categorize",
    "client": "client_name",
    "client_sub_name": "client_code",
    "uses": "uses",
    "purpose": "purpose",
    "campaign_type": "campaign_type",
    "purpose_to_use": "purpose_use",
    "campaign_type_to_use": "campaign_type_use",
    "target_user": "target_user",
}

app_name_to_use = ', '.join(list(app_name_to_use_dict_column_name.values()))
QUERY_TO_UB_RAWDATA_URBOX_APP_NAME_TO_USE = f"INSERT into ub_rawdata.app_name_to_use ({app_name_to_use}) SELECT {app_name_to_use} from sentry.app_name_to_use"
