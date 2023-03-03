import json
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
import pandas as pd
import streamlit as st

def create_session_object():
   
    with open('config.json') as f:
        connection_parameters = json.load(f)
        session = Session.builder.configs(connection_parameters).create()
        return session


# Create Snowpark DataFrames that loads data from Knoema: Environmental Data Atlas
def load_data(session):
    # CO2 Emissions by Country
    snow_df_co2 = session.table("WAREHOUSE_METERING_HISTORY")
    snow_df_co2 = snow_df_co2.group_by('WAREHOUSE_NAME').agg(sum('$5').alias("Total Credit Consumption"))

    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_co2 = snow_df_co2.to_pandas()

    col1, col2, col3 = st.columns(3)
    with st.container():
        with col1:
            st.subheader('Credit Consumption')
            st.dataframe(pd_df_co2)
    

if __name__ == "__main__":
    session = create_session_object()
    load_data(session)
