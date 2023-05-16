import streamlit as st
import pandas as pd
import os

st.title('신 마법 공식')
dataframe = pd.read_csv("/app/Data/마법공식/마법공식.csv", index_col=0)
dataframe.index = dataframe.index.astype(str).str.zfill(6)
dataframe.sort_values(by="시가총액(억 원)", ascending=False, inplace=True)
st.dataframe(dataframe, height=1000, width=1500)


