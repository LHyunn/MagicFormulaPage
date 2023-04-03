import streamlit as st
import datetime
import modules.public_stock as public_stock


def set_config():
    """
    Streamlit 기본 설정.
    """
    st.set_page_config(
        page_title="마법 공식",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
        'About': "제작자 : 이창현, https://github.com/LHyunn"
    }
    )
    
def init_session():
    with st.expander(f"KRX 종목코드 업데이트 - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"):
        kosdaq_update_time = public_stock.update_kosdaq_stock_code()
        kospi_update_time = public_stock.update_kospi_stock_code()
        st.success(f"KOSPI 마지막 업데이트 시간 : {kospi_update_time}")
        st.success(f"KOSDAQ 마지막 업데이트 시간 : {kosdaq_update_time}")
        
        
    
    

    
    
def set_sidebar():
    """
    Streamlit 사이드바 설정.
    """
    st.sidebar.title("2023 캡스톤디자인")

    menu = st.sidebar.selectbox("",("Home", "Data", "Model", "Result"))

    return menu
