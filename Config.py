# Redis Connection
REDIS_CONFIG_STAGING = {
    "host": "10.0.1.22",
    "port": "6380",
    "db": "12"
}

# MySQL Connection
MYSQL_CONFIG_STAGING = {
    "host": "urbox-staging.ckkags9rp5be.ap-southeast-1.rds.amazonaws.com",
    "port": "3306",
    "user": "ura12_sync",
    "password": "GOwysskaDEQYFNTUq2It",
    "auth_plugin": "mysql_native_password"
}
MYSQL_CONFIG_PRODUCTION = {
    "host": "urbox.ckkags9rp5be.ap-southeast-1.rds.amazonaws.com",
    "port": "3306",
    "user": "ura12_urbox",
    "password": "4Q1j?e%7aVi3",
    "auth_plugin": "mysql_native_password"
}

# Redshift Connection
REDSHIFT_CONFIG = {
    "host": "urbox-dw-cluster.cwf129wsbvw0.ap-southeast-1.redshift.amazonaws.com",
    "port": "5439",
    "dbname": "urbox",
    "user": "airflow_user",
    "password": "RB25yvtUMZdyRXbg"
}

# Email
EMAIL_SENDER_NAME = "Team Data"
EMAIL_SENDER_ADDRESS = "datateam@urbox.vn"
EMAIL_SENDER_PASSWORD = "34d#7#39H5^E%^ck"
