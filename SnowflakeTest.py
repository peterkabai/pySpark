import snowflake.connector
import os

USERNAME = os.getenv('SNOWSQL_USR')
PASSWORD = os.getenv('SNOWSQL_PWD')
ACCOUNT = os.getenv('SNOWSQL_ACC')
WAREHOUSE = os.getenv('SNOWSQL_WAR')

conn = snowflake.connector.connect(
    user=USERNAME,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE
    )
cs = conn.cursor()

create_table = """
    CREATE OR REPLACE TABLE TEST_DB.TEST_SCHEMA.NEW_TABLE (
        col_1 varchar(16777216), 
        col_2 int,  
        col_3 int
    )"""
cs.execute(create_table)
