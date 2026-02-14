import streamlit as st
from pages.utils import data
import pandas as pd
import decimal

data = data.get_sql("sample.default.gold_agg_envirmentalactivity")
#data = pd.read_csv("flox_agg_data.csv")
for col in data.select_dtypes(include='object'):
    if data[col].apply(lambda x: isinstance(x, decimal.Decimal)).any():
        data[col] = data[col].astype(float)

st.markdown("<h1 style='text-align: center; color: grey;'>FLOX</h1>", unsafe_allow_html=True)
st.divider()
st.markdown("<h3 style='text-align: center; color: darkblue;'>Temprature</h3>", unsafe_allow_html=True)


st.line_chart(data,x="AVG_TEMPRATURE",y=["AVG_BIRD_ACTIVITY","AVG_SOUND_LEVEL"])


st.markdown("<h3 style='text-align: center; color: darkblue;'>Humidity</h3>", unsafe_allow_html=True)


st.line_chart(data,x="AVG_HUMIDITY",y=["AVG_BIRD_ACTIVITY","AVG_SOUND_LEVEL"])
