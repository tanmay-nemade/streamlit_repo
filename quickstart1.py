# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd

def create_session_object():
    connection_parameters = st.secrets["snowflake"]
    session = Session.builder.configs(connection_parameters).create()
    return session

  
# Create Snowpark DataFrames that loads data from Knoema: Environmental Data Atlas
def load_data(session):
    snow_df_co2 = session.table("SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY")\
                         .select(col("WAREHOUSE_NAME"),col("CREDITS_USED")).collect()
    
    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_co2  = pd.DataFrame(snow_df_co2)
    
    st.write(pd_df_co2)
    

if __name__ == "__main__":
    session = create_session_object()
    load_data(session)
