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
    # CO2 Emissions by Country
    raw_wh_df =   session.table("WAREHOUSE_METERING_HISTORY")\
                    .select(concat(year(col("START_TIME")),month(col("START_TIME"))).as_("Year-Month"),col("WAREHOUSE_NAME"),lit(col("CREDITS_USED")).cast(DecimalType(10,0)).alias("CREDITS_USED"))
                   #.select(date_trunc("MONTH",col("START_TIME")).as_("QUERY_MONTH"),col("WAREHOUSE_NAME"),lit(col("CREDITS_USED")).cast(DecimalType(10,0)).alias("CREDITS_USED"))
                    #.select(col("WAREHOUSE_NAME"),to_decimal(col("CREDITS_USED"),10,0).as_("CREDITS_USED"))
    raw_wh_df = raw_wh_df.filter(col("WAREHOUSE_NAME").isNotNull())
    raw_wh_df = raw_wh_df.filter(col("CREDITS_USED") > 0)

    
    snow_df_co2 = raw_wh_df.group_by(col("WAREHOUSE_NAME")).agg(sum('CREDITS_USED').as_("Total Credit Consumption"))
    snow_df_co2 = snow_df_co2.select(col("WAREHOUSE_NAME").as_("Warehouse Name"),col("Total Credit Consumption"))
    snow_df_co2 = snow_df_co2.sort(col("Total Credit Consumption").desc())
    snow_df_co2 = snow_df_co2.limit(10)
    #snow_df_co2 = snow_df_co2.to_pandas()

    monthly_df = raw_wh_df.group_by(col("Year-Month")).agg(sum('CREDITS_USED').as_("Total Credit Consumption"))
    monthly_df = monthly_df.filter(col("Total Credit Consumption") > 0)
    monthly_df = monthly_df.sort(col("Year-Month").desc())


    # Convert Snowpark DataFrames to Pandas DataFrames for Streamlit
    #snow_df_co2 = snow_df_co2.to_pandas()

    col1, col2 = st.columns(2)
    with st.container():
        with col1:
            st.markdown(":red[Monthly Credit Consumtpion - Table]")
            st.dataframe(monthly_df)
            st.write('Warehouse  Credit Consumtpion - Table')
            st.dataframe(snow_df_co2)
            
                       
            
        with col2:
            st.write('Monthly Credit Consumtpion - Graph')
            st.bar_chart(monthly_df,x="Year-Month",y="Total Credit Consumption")
            st.write('wWarehouse Credit Consumtpion - Graph')
            st.bar_chart(snow_df_co2,x="Warehouse Name",y="Total Credit Consumption")
            #edited_df = st.experimental_data_editor(snow_df_co2)                  
            
            
if __name__ == "__main__":
    session = create_session_object()
    wh_data(session)