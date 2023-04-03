import datetime
import os 
import urllib.request
import ssl
import zipfile
import os
import pandas as pd
from glob import glob


def get_data(annual, consolidated, document, time):
    """
    annual: 분기/연간
    consolidated: 별도/연결
    document: 재무제표/포괄손익계산서
    time: 연도 + 분기
    """
    