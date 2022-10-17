import os

local_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(local_dir)
gsheet_cridential_path_user_urbox = os.path.join(local_dir, "urbox_credentials.json")
gsheet_token_path_user_urbox = os.path.join(local_dir, "urbox_token.pickle")
