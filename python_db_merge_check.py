#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#기본 import 함수들
import numpy as np
import pandas as pd
from pandas import DataFrame, Timestamp
from collections import Counter
import os

import time
from datetime import datetime, timedelta


# In[ ]:


def checking_million_count(SRC_sql, SRC_con, counting=1000000):
    # count먼저 수행해서 오래 걸리는거만 걸러내기

    SRC_count_query = "SELECT COUNT(*) FROM"  #맨 앞에만 count를 적용하기 위함
    SRC_only_count_use = SRC_sql.split('FROM')
    for iii in range(1, len(SRC_only_count_use) - 1):
        SRC_count_query = SRC_count_query + SRC_only_count_use[iii] + "FROM"
    SRC_count_query = SRC_count_query + SRC_only_count_use[-1]

    try:
        SRC_count_result = pd.read_sql(SRC_count_query, con=SRC_con)
        if SRC_count_result.iloc[0][0] >= counting:
            result = [
                counting, counting, 'SRC_테이블 count ' + str(counting) + '건 이상',
                '인코딩 미 확인',
                datetime.now()
            ]
            return result
        else:
            return None
    except:
        result = [
            -9999, -9999, 'SRC_count_SQL READ ERROR', '인코딩 미 확인',
            datetime.now()
        ]
        return result


def comparing_column(SRC_result, TGT_result):
    SRC_tm, TGT_tm = SRC_result.columns, TGT_result.columns

    if sorted(SRC_tm) != sorted(TGT_tm):
        SRC_sub_TGT = [x for x in SRC_tm if x not in TGT_tm]
        TGT_sub_SRC = [x for x in TGT_tm if x not in SRC_tm]

        SRC1 = ''
        for jkl in SRC_sub_TGT:
            SRC1 = SRC1 + jkl + ', '
        SRC_note = '소스 데이터에 ' + SRC1[:-2] + " 컬럼이 추가로 있습니다."
        TGT1 = ''
        for jkl in TGT_sub_SRC:
            TGT1 = TGT1 + jkl + ', '
        TGT_note = '타겟 데이터에 ' + TGT1[:-2] + " 컬럼이 추가로 있습니다."

        if SRC1[:-2] == '':
            note = TGT_note
        elif TGT1[:-2] == '':
            note = SRC_note
        else:
            note = SRC_note + ' ' + TGT_note

        result = [-9999, -9999, note, '인코딩 미 확인', datetime.now()]
    else:
        result = None

    return result


# In[ ]:


def type_check_change(DB_Name, SCHEMA, Table_Name, sql_con, sql_result,
                      SRC_tm):

    if DB_Name == 'DB2':  #DB2
        sql_type_check_query = "select colname, typename from syscat.columns where tabschema = '" + SCHEMA + "' and tabname = '" + Table_Name + "'"
        sql_type_check = pd.read_sql(sql_type_check_query, con=sql_con)
        sql_type_check = sql_type_check.rename(columns={
            "COLNAME": "COLUMN_NAME",
            "TYPENAME": "DATA_TYPE"
        })
    elif DB_Name == 'POSTGRE':  #postgre
        gp1 = "select a.attname, c.typname from   pg_catalog.pg_attribute a, pg_catalog.pg_class b, pg_catalog.pg_type c, pg_catalog.pg_tables d"
        gp2 = " where a.attrelid = b.oid and a.atttypid = c.oid and b.relname  = d.tablename and a.attnum > 0 and d.tablename = '"
        sql_type_check_query = gp1 + gp2 + Table_Name.lower(
        ) + "' and d.schemaname = '" + SCHEMA + "'"
        sql_type_check = pd.read_sql(sql_type_check_query, con=sql_con)
        sql_type_check = sql_type_check.rename(columns={
            "attname": "COLUMN_NAME",
            "typname": "DATA_TYPE"
        })
        sql_type_check['COLUMN_NAME'] = sql_type_check[
            'COLUMN_NAME'].str.upper()
        sql_type_check['DATA_TYPE'] = sql_type_check['DATA_TYPE'].str.upper()
    else:  #oracle
        sql_type_check_query = "SELECT column_name, data_type FROM all_tab_columns where table_name =  '" + Table_Name + "' AND OWNER = '" + SCHEMA + "'"
        sql_type_check = pd.read_sql(sql_type_check_query, con=sql_con)

    sql_type_check = sql_type_check[sql_type_check['DATA_TYPE'] != 'UNDEFINED']
    sql_type_check = sql_type_check[sql_type_check['COLUMN_NAME'].isin(
        SRC_tm)].drop_duplicates(keep='first').reset_index(drop=True)

    for i in range(sql_type_check.shape[0]):
        try:
            if sql_type_check['DATA_TYPE'][i] in [
                    'FLOAT', 'SMALLINT', 'NUMBER', 'INTEGER', 'BIGINT',
                    'DECIMAL', 'NUMERIC', 'REAL', 'DOUBLE'
            ]:
                sql_result[sql_type_check['COLUMN_NAME'][i]] = sql_result[
                    sql_type_check['COLUMN_NAME'][i]].fillna(-999)
                sql_result[sql_type_check['COLUMN_NAME'][i]] = pd.to_numeric(
                    sql_result[sql_type_check['COLUMN_NAME'][i]])
                sql_result[sql_type_check['COLUMN_NAME'][i]] = [
                    -999 if i == None else i
                    for i in sql_result[sql_type_check['COLUMN_NAME'][i]]
                ]
            elif sql_type_check['DATA_TYPE'][i] in ['DATE', 'DATETIME']:
                sql_result[sql_type_check['COLUMN_NAME'][i]] = sql_result[
                    sql_type_check['COLUMN_NAME'][i]].fillna(
                        pd.to_datetime('1990-01-01'))
                sql_result[sql_type_check['COLUMN_NAME'][i]] = [
                    pd.to_datetime('1990-01-01') if i == None else i
                    for i in sql_result[sql_type_check['COLUMN_NAME'][i]]
                ]
            elif sql_type_check['DATA_TYPE'][i].find('TIMESTAMP') >= 0:
                sql_result[sql_type_check['COLUMN_NAME'][i]] = sql_result[
                    sql_type_check['COLUMN_NAME'][i]].fillna(
                        pd.Timestamp(1990, 1, 1, 0, 0, 0))
                sql_result[sql_type_check['COLUMN_NAME'][i]] = [
                    pd.Timestamp(1990, 1, 1, 0, 0, 0) if i == None else i
                    for i in sql_result[sql_type_check['COLUMN_NAME'][i]]
                ]
            else:
                sql_result[sql_type_check['COLUMN_NAME'][i]] = sql_result[
                    sql_type_check['COLUMN_NAME'][i]].fillna("대체").replace(
                        '', "대체").replace(' ', "대체").replace('-', "대체")
                sql_result[sql_type_check['COLUMN_NAME'][i]] = [
                    '대체' if i == None else i
                    for i in sql_result[sql_type_check['COLUMN_NAME'][i]]
                ]

            if sql_type_check['DATA_TYPE'][i].find('TIMESTAMP') >= 0:
                sql_result[sql_type_check['COLUMN_NAME'][i]] = [
                    i.strftime('%Y-%m-%d-%H:%M:%S')
                    for i in sql_result[sql_type_check['COLUMN_NAME'][i]]
                ]
            elif sql_type_check['DATA_TYPE'][i].find('DATE') >= 0:
                sql_result[sql_type_check['COLUMN_NAME'][i]] = [
                    i.strftime('%Y-%m-%d-%H:%M:%S')
                    for i in sql_result[sql_type_check['COLUMN_NAME'][i]]
                ]
            else:
                pass

        except:
            pass

    return sql_result, sql_type_check


# In[ ]:


# 반환 받은 SRC, TGT를 merge해서 결과를 노트에 넣는 함수
def source_target_merge(SRC_result, TGT_result):
    try:
        total_result = pd.merge(SRC_result,
                                TGT_result,
                                how='outer',
                                indicator=True)
    except:
        result = [
            -9999, -9999, 'dataframe merge error. please, check data type',
            '인코딩 미 확인',
            datetime.now()
        ]
    else:
        left = Counter(total_result['_merge'])['left_only']
        right = Counter(total_result['_merge'])['right_only']
        both = Counter(total_result['_merge'])['both']
        t_count = total_result.shape[0]

        if ((left == 0) & (right == 0) & (both == 0)) == True:
            note = 'SRC, TGT AS-IS 0건'
        elif ((left == 0) & (right == 0)) == True:
            note = str(t_count) + '건 모두 일치'
        elif ((left == 0) & (right != 0)) == True:
            note = 'TGT 테이블만 갯수 많음'
        elif ((left != 0) & (right == 0)) == True:
            note = 'SRC 테이블만 갯수 많음'
        else:
            note = '데이터 불일치'

        result = [left, right, note]

    return result, total_result


def source_target_merge_oracle_postgre(SRC_result, SRC_type_check,
                                       SRC_table_name, TGT_result,
                                       TGT_type_check, TGT_table_name):
    try:
        result, total_result = source_target_merge(SRC_result, TGT_result)
        return result, total_result
    except:
        abc = pd.merge(SRC_type_check, TGT_type_check, on='COLUMN_NAME')
        for it in range(abc.shape[0]):
            if (abc['DATA_TYPE_x'][it] == 'NUMBER') & (abc['DATA_TYPE_y'][it]
                                                       == 'VARCHAR'):
                SRC_result[abc['COLUMN_NAME'][it]] = SRC_result[
                    abc['COLUMN_NAME'][it]].astype(str)
                SRC_result[abc['COLUMN_NAME'][it]] = SRC_result[
                    abc['COLUMN_NAME'][it]].replace("-999", "대체")
            elif (abc['DATA_TYPE_x'][it].find("TIMESTAMP") >=
                  0) & (abc['DATA_TYPE_y'][it] == 'DATE'):
                SRC_result[abc['COLUMN_NAME'][it]] = SRC_result[
                    abc['COLUMN_NAME'][it]].astype(str)
                SRC_result[abc['COLUMN_NAME'][it]] = [
                    i[0:10] for i in SRC_result[abc['COLUMN_NAME'][it]]
                ]
                SRC_result[abc['COLUMN_NAME'][it]] = SRC_result[
                    abc['COLUMN_NAME'][it]].replace('1990-01-01', "대체")
                TGT_result[abc['COLUMN_NAME'][it]] = TGT_result[
                    abc['COLUMN_NAME'][it]].astype(str)
                TGT_result[abc['COLUMN_NAME'][it]] = TGT_result[
                    abc['COLUMN_NAME'][it]].replace('1990-01-01', "대체")
            elif (abc['DATA_TYPE_x'][it]
                  == 'VARCHAR2') & (abc['DATA_TYPE_y'][it] == 'NUMERIC'):
                try:
                    SRC_result[abc['COLUMN_NAME'][it]] = SRC_result[
                        abc['COLUMN_NAME'][it]].replace("대체", -999)
                    SRC_result[abc['COLUMN_NAME'][it]] = SRC_result[
                        abc['COLUMN_NAME'][it]].astype(int)
                except:
                    pass
            else:
                pass
        try:
            result, total_result = source_target_merge(SRC_result, TGT_result)
            return result, total_result
        except:
            result = [
                -9999, -9999, 'dataframe merge error. please, check data type',
                '인코딩 미 확인',
                datetime.now()
            ]
            total_result = abc
            total_result.columns = [
                'COLUMN_NAME', 'SRC_COLUMN_TYPE', 'TGT_COLUMN_TYPE'
            ]
            pass
        pass
    return result, total_result


def checking_encoding(fail_list, result, SRC_result, TGT_result):

    SRC_encoding, TGT_encoding = [], []

    for col_col in SRC_result.columns:
        for jj in fail_list:
            try:
                if SRC_result[col_col].str.contains(jj).sum() > 0:
                    SRC_encoding.append(col_col + jj)
            except:
                pass
    for col_col in TGT_result.columns:
        for jj in fail_list:
            try:
                if TGT_result[col_col].str.contains(jj).sum() > 0:
                    TGT_encoding.append(col_col + jj)
            except:
                pass

    if len(SRC_encoding) > 0 and len(TGT_encoding) > 0:
        result.append("SRC TGT 인코딩 확인 바람")
    elif len(SRC_encoding) > 0 and len(TGT_encoding) == 0:
        result.append("SRC 인코딩 확인 바람")
    elif len(SRC_encoding) == 0 and len(TGT_encoding) > 0:
        result.append("TGT 인코딩 확인 바람")
    else:
        result.append("인코딩 이상 없음")

    result.append(datetime.now())

    return result


# In[ ]:


def total_merge_check(SRC_db_name,
                      SRC_SCHEMA,
                      SRC_table_name,
                      SRC_sql,
                      SRC_con,
                      TGT_db_name,
                      TGT_SCHEMA,
                      TGT_table_name,
                      TGT_sql,
                      TGT_con,
                      counter=1000000,
                      fail_list=['⑦', ' ', '⑹', 'ㅼ', 'ъ']):

    SRC_sql = SRC_sql.replace(';',
                              '')  #쿼리 마지막에 있는 ;가 있으면 쿼리 자체가 실행이 안되는 경우가 있어 제거함
    TGT_sql = TGT_sql.replace(';', '')
    SRC_note, TGT_note = '', ''
    result_columns = [
        'SRC_not_equal', 'TGT_not_equal', 'result_note', 'table_encoding',
        'checking_time'
    ]

    result = checking_million_count(SRC_sql, SRC_con, counting=counter)

    try:
        SRC_result = pd.read_sql(SRC_sql, con=SRC_con)
        SRC_result.columns = map(lambda x: str(x).upper(), SRC_result.columns)
        SRC_result_backup = SRC_result.copy()
    except:
        SRC_note = "SRC SQL ERROR"
    try:
        TGT_result = pd.read_sql(TGT_sql, con=TGT_con)
        TGT_result.columns = map(lambda x: str(x).upper(), TGT_result.columns)
        TGT_result_backup = TGT_result.copy()
    except:
        TGT_note = "TGT SQL ERROR"

    if SRC_note == "" and TGT_note == "":
        pass
    elif SRC_note == "" and TGT_note != "":
        result = [
            SRC_result.shape[0], -9999, TGT_note, '인코딩 미 확인',
            datetime.now()
        ]
    elif SRC_note != "" and TGT_note == "":
        result = [
            -9999, TGT_result.shape[0], SRC_note, '인코딩 미 확인',
            datetime.now()
        ]
    else:
        result = [-9999, -9999, "ALL SQL ERROR", '인코딩 미 확인', datetime.now()]

    for i in range(1):
        if result != None:
            return DataFrame([result], columns=result_columns), DataFrame()
            continue

    SRC_tm, TGT_tm = SRC_result.columns, TGT_result.columns

    result = comparing_column(SRC_result, TGT_result)

    for i in range(1):
        if result != None:
            return DataFrame([result], columns=result_columns), DataFrame()
            continue

    #DB2, Oracle 타입별로 결측치 채우기
    SRC_result = SRC_result[SRC_tm]
    TGT_result = TGT_result[TGT_tm]

    SRC_result, SRC_type_check = type_check_change(SRC_db_name, SRC_SCHEMA,
                                                   SRC_table_name, SRC_con,
                                                   SRC_result, SRC_tm)
    TGT_result, TGT_type_check = type_check_change(TGT_db_name, TGT_SCHEMA,
                                                   TGT_table_name, TGT_con,
                                                   TGT_result, TGT_tm)

    if SRC_db_name == 'ORACLE' and TGT_db_name == 'POSTGRE':
        result, total_result = source_target_merge_oracle_postgre(
            SRC_result, SRC_type_check, SRC_table_name, TGT_result,
            TGT_type_check, TGT_table_name)
    else:
        result, total_result = source_target_merge(SRC_result, TGT_result)

    for i in range(1):
        if len(result) == 5:
            return DataFrame([result], columns=result_columns), total_result
            continue

    result = checking_encoding(fail_list, result, SRC_result, TGT_result)

    return DataFrame([result],
                     columns=result_columns), total_result.sort_values(
                         list(total_result.columns))

