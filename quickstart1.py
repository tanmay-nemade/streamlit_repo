# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import avg, sum, col,lit
import streamlit as st
import pandas as pd

st.set_page_config(
     page_title="Environment Data Atlas",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Data Marketplace"
     }
)

# Create Session object
def create_session_object():
    connection_parameters = {
      "account": "nq94943.ap-south-1.aws",
      "user": "mathavale",
      "password": "Infosys123",
      "role": "accountadmin",
      "warehouse": "compute_wh",
      "database" : "snowflake",
      "schema" : "account_usage"
    }
    session = Session.builder.configs(connection_parameters).create()
    #print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
    return session

  
# Create Snowpark DataFrames that loads data from Knoema: Environmental Data Atlas
def load_data(session):
    snow_df_co2 = session.table("SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY").collect()
    
    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    pd_df_co2  = pd.to_pandas(snow_df_co2)
    
    st.write(pd_df_co2)
    

if __name__ == "__main__":
    session = create_session_object()
    load_data(session)
