import streamlit as st
import pandas as pd
from pages.utils import data

#sys.path.append(str(pathlib.Path(__file__)))

data = data.get_sql("sample.default.gold_agg_envirmentalactivity")
#data = data.get_aggregate()
#data = pd.read_csv("flox_agg_data.csv")

st.markdown("<h1 style='text-align: center; color: grey;'>FLOX</h1>", unsafe_allow_html=True)
st.divider()
st.markdown("<h3 style='text-align: center; color: darkblue;'>Environmental effect on activity stats</h3>", unsafe_allow_html=True)

st.write(data)