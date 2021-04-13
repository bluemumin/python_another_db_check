#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#기본 import 함수들
import numpy as np
import pandas as pd
from pandas import DataFrame, Timestamp
import os
from datetime import datetime, timedelta


# In[ ]:


def SRC_TGT_count_same(SRC_query, SRC_con, TGT_query, TGT_con):
    SRC_query  = SRC_query.replace(';','') #쿼리 마지막에 있는 ;가 있으면 쿼리 자체가 실행이 안되는 경우가 있어 제거함
    TGT_query  = TGT_query.replace(';','')
    
    result = []
    try:
        SRC_one_result = pd.read_sql(SRC_query, con = SRC_con )
    except:
        result.append(-9999)
        SRC_note = "SRC SQL ERROR"
    else:
        result.append(SRC_one_result.iloc[0][0])
        SRC_note = "이상없음"
    try:
        TGT_one_result = pd.read_sql(TGT_query, con = TGT_con )
    except:
        result.append(-9999)
        TGT_note = "TGT SQL ERROR"
    else:
        result.append(TGT_one_result.iloc[0][0])
        TGT_note = "이상없음"

    if SRC_note == "이상없음" and TGT_note == "이상없음":
        result.append("모든 쿼리 정상 수행")
        result.append(SRC_one_result.iloc[0][0] - TGT_one_result.iloc[0][0])
    elif SRC_note == "이상없음" and TGT_note != "이상없음":
        result.append(TGT_note)
        result.append(-9999)
    elif SRC_note != "이상없음" and TGT_note == "이상없음":
        result.append(SRC_note)
        result.append(-9999)
    else :
        result.append("ALL SQL ERROR")
        result.append(-9999)
        
    result.append(datetime.now())
    result2 = DataFrame([result], columns=['SRC_count','TGT_count','Query_Error','minus','checking_time'])
    
    return result2

