from modules import config, public_stock
import streamlit as st
import os
import pandas as pd


if __name__ == "__main__":
    config.set_config()
    st.title("마법공식")
    config.init_session()
    col1, col2, col3 = st.columns(3)
    annual = col1.selectbox("분기/연간", os.listdir("/app/Data/Info/Sort By Time"))
    consolidated = col2.selectbox("별도/연결", os.listdir(f"/app/Data/Info/Sort By Time/{annual}"))
    year = col3.selectbox("연도", os.listdir(f"/app/Data/Info/Sort By Time/{annual}/{consolidated}"))
    dataframe = pd.read_csv(f"/app/Data/Info/Sort By Time/{annual}/{consolidated}/{year}")
    #첫번째 열을 문자열로 변환
    dataframe.iloc[:,0] = dataframe.iloc[:,0].astype(str)
    dataframe.iloc[:,0] = dataframe.iloc[:,0].str.zfill(6)
    #첫번째 열의 이름을 종목코드로 변경
    dataframe.rename(columns={dataframe.columns[0]:"종목코드"}, inplace=True)
    #종목코드를 인덱스로 설정
    dataframe.set_index("종목코드", inplace=True)
    st.dataframe(dataframe, height=2000)
    
    
    
    
    
    
    
    

