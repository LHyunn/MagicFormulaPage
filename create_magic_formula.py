import pandas as pd
import os
import numpy as np



def main():
    print("마법공식 생성 시작.")
    df = pd.DataFrame(columns=['종목명','시가총액(억 원)','부채율(%)','배당률(%)','PBR','GP/A'])
    #재무제표 데이터 불러오기
    report_folder = "/app/Data/Info/Sort By Time/연간/연결"
    report_df = pd.read_csv(report_folder + "/" + "2022-12-재무제표-연결-연간.csv", encoding='utf-8-sig', index_col=0)
    report_df.index = report_df.index.astype(str)
    #zerofill 6
    report_df.index = report_df.index.str.zfill(6)
    #포괄손익계산서 데이터 불러오기
    report_folder = "/app/Data/Info/Sort By Time/연간/연결"
    report2_df = pd.read_csv(report_folder + "/" + "2022-12-포괄손익계산서-연결-연간.csv", encoding='utf-8-sig', index_col=0)
    report2_df.index = report2_df.index.astype(str)
    report2_df.index = report2_df.index.str.zfill(6)
    #종목 데이터 불러오기
    stock_folder = "/app/Data/KRX"
    kospi_df = pd.read_csv(stock_folder + "/" + "kospi_stock_info.csv", encoding='utf-8-sig', index_col=0)
    kospi_df.index = kospi_df.index.astype(str)
    kospi_df.index = kospi_df.index.str.zfill(6)
    kosdaq_df = pd.read_csv(stock_folder + "/" + "kosdaq_stock_info.csv", encoding='utf-8-sig', index_col=0)
    kosdaq_df.index = kosdaq_df.index.astype(str)
    kosdaq_df.index = kosdaq_df.index.str.zfill(6)
    print("데이터 불러오기 완료.")
    print("종목별 데이터 생성.")
    for stock_code in report_df.index:
        try:
            종목명 = kospi_df.loc[stock_code]['한글명']
            시가총액 = kospi_df.loc[stock_code]['시가총액']
            부채비율 = report_df.loc[stock_code]['부채비율']
            배당수익률 = report_df.loc[stock_code]['배당수익률']
            PBR = report_df.loc[stock_code]['PBR']
            매출액 = report2_df.loc[stock_code]['매출액']
            매출원가 = report2_df.loc[stock_code]['매출원가']
            매출총이익 = 매출액 - 매출원가
            자산총계 = report_df.loc[stock_code]['자산총계']
            GPA = 매출총이익 / 자산총계 * 100
            df.loc[stock_code] = [종목명, 시가총액, 부채비율, 배당수익률, PBR, GPA]
        except:
            try:
                종목명 = kosdaq_df.loc[stock_code]['한글종목명']
                시가총액 = kosdaq_df.loc[stock_code]['전일기준 시가총액 (억)']
                부채비율 = report_df.loc[stock_code]['부채비율']
                배당수익률 = report_df.loc[stock_code]['배당수익률']
                PBR = report_df.loc[stock_code]['PBR']
                매출액 = report2_df.loc[stock_code]['매출액']
                매출원가 = report2_df.loc[stock_code]['매출원가']
                매출총이익 = 매출액 - 매출원가
                자산총계 = report_df.loc[stock_code]['자산총계']
                GPA = 매출총이익 / 자산총계
                df.loc[stock_code] = [종목명, 시가총액, 부채비율, 배당수익률, PBR, GPA]
            except:
                continue
            
    df.sort_values(by=['시가총액(억 원)'], ascending=False)
    df.dropna(inplace=True)
    if not os.path.exists("/app/Data/마법공식"):
        os.makedirs("/app/Data/마법공식")
        
    if os.path.exists("/app/Data/마법공식/마법공식.csv"):
        os.remove("/app/Data/마법공식/마법공식.csv")
    df.to_csv("/app/Data/마법공식/마법공식.csv", encoding='utf-8-sig')
    print("마법공식 생성 완료.")
        
        
if __name__ == "__main__":
    main()
            
   
    
