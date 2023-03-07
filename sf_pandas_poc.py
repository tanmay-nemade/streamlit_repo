import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st
from datetime import datetime
from snowflake.snowpark.types import DecimalType

st.set_page_config(layout="wide")
st.header("Snowflake Monitoring Framework")


def create_session_object():
    connection_parameters = st.secrets["snowflake"]
    session = Session.builder.configs(connection_parameters).create()
    return session


# Create Snowpark DataFrames that loads data from Knoema: Environmental Data Atlas
def wh_data(session):
    
    raw_wh_df =   session.sql("SELECT WAREHOUSE_NAME,SUM(CREDITS_USED) AS TOTAL_CREDITS FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY GROUP BY WAREHOUSE_NAME ORDER BY TOTAL_CREDITS DESC;")

    pd_raw_wh_df = raw_wh_df.to_pandas()

    st.dataframe(pd_raw_wh_df)
            
            
if __name__ == "__main__":
    session = create_session_object()
    wh_data(session)
