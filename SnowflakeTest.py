import snowflake.connector
import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

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
        name varchar(16777216), 
        age int,  
        weight int
    )"""
cs.execute(create_table)

df = pd.DataFrame([
    ('Mark', 10, 100),
    ('Luke', 20, 200),
    ('Peter', 30, 210)
    ], columns=['NAME', 'AGE', 'WEIGHT'])
# when the table is created the column names are capitalized
# this means that the column names in the df have to be capitalized, otherwise we get:
# snowflake.connector.errors.ProgrammingError [...] SQL compilation error [...] invalid identifier


write_pandas(conn, df, "NEW_TABLE", "TEST_DB", "TEST_SCHEMA")
# I was having pandas dependency issues with this. I fixed it with:
# pip3 install "snowflake-connector-python[pandas]"

cs.execute("SELECT * FROM TEST_DB.TEST_SCHEMA.NEW_TABLE LIMIT 10")
rows = cs.fetchall()
for row in rows:
    print(row)

recieved_df = cs.fetch_pandas_all()
print(recieved_df)

cs.close()
conn.close()
