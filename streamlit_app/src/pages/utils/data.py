import streamlit as st
from databricks import sql
import os
from pandas import DataFrame

def get_sql(tbl_name):  

    connection = sql.connect(
                            server_hostname = st.secrets.db_credentials.host,
                            http_path = st.secrets.db_credentials.path,
                            access_token = st.secrets.db_credentials.token)
    cursor = connection.cursor()

    cursor.execute(f"SELECT * from  {tbl_name}")
    columns = [desc[0] for desc in cursor.description]
    df_result = DataFrame(cursor.fetchall(), columns=columns)
    cursor.close()
    connection.close()
    return df_result
