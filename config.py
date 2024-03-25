import os

OS_DB_CREDENTIAL_HOST = 'DB_HOST'
OS_DB_CREDENTIAL_PORT = 'DB_PORT'
OS_DB_CREDENTIAL_DATABASE_NAME = 'DB_NAME'
OS_DB_CREDENTIAL_USER_NAME = 'DB_USER'
OS_DB_CREDENTIAL_PASSWORD = 'DB_PASSWORD'

TARGET_URL = 'https://jsonplaceholder.typicode.com/posts/'
TARGET_STG_TABLE_NAME = 'raw_test_data'
TARGET_STG_SCHEMA_NAME = 'stg'

TARGET_DDS_TABLE_HUB_USERS = 'h_users'
TARGET_DDS_TABLE_HUB_LETTERS = 'h_letters'
TARGET_DDS_TABLE_SATELLITE_LETTERS = 's_letters'
TARGET_DDS_TABLE_LINK_POSTS = 'l_posts'
TARGET_DDS_SCHEMA_NAME = 'dds'
TIMEOUT = 60

host_name = os.getenv(OS_DB_CREDENTIAL_HOST) or 'localhost' 
port = os.getenv(OS_DB_CREDENTIAL_PORT) or '5439'
database = os.getenv(OS_DB_CREDENTIAL_DATABASE_NAME) or 'test_db_name'
user = os.getenv(OS_DB_CREDENTIAL_USER_NAME) or 'postgres'
password = os.getenv(OS_DB_CREDENTIAL_PASSWORD) or 'postgres'

CONN_STR = f'postgresql://{user}:{password}@{host_name}:{port}/{database}'