# python_another_db_check
DBMS간 건수, 데이터 정합성 검증용 함수 python 파일

이 파일은 DB2, Oracle, Postgre, sql server(MS SQL)을 연결한 이후,

(DBMS 통합 연결 함수 : https://github.com/bluemumin/python_db_connection)

서로 다른 DBMS간에 바로 건수검증, 정합성 검증을 수행하고자 만든 검증용 함수입니다.

정합성 검증 방식 요약

![db_check_설명용](https://user-images.githubusercontent.com/53479967/115150936-8832a300-a0a5-11eb-8385-7c738c5f4c65.PNG)

--------------------------------------------------------------

A. 건수 검증 실제 수행 코드

## 1. 검증하고자 하는 db 연결하기
from python_db_count_check import SRC_TGT_count_same

from python_db_con import python_db_connection

oracle_install_space = "C:\oracle\instantclient_19_8"

SRC_con = python_db_connection('db종류!=oracle', 'DB명칭','host','port','id','pw')

TGT_con = python_db_connection('db종류=oracle', 'DB명칭','host','port','id','pw',oracle_install_space)

## 2. 검증하고자 하는 쿼리 생성하기
SRC_query = "select count(*) 시작 쿼리1"

TGT_query = "select count(*) 시작 쿼리2"

## 3. 1,2번 객체들을 함수에 넣고 건수 검증 수행
SRC_TGT_count_same(SRC_query, SRC_con, TGT_query, TGT_con)

--------------------------------------------------------------

B. 정합성 검증 실제 수행 코드

## 1. 검증하고자 하는 db 연결하기
from python_db_merge_check import total_merge_check

from python_db_con import python_db_connection

oracle_install_space = "C:\oracle\instantclient_19_8"

SRC_con = python_db_connection('db종류!=oracle', 'DB명칭', 'host', 'port', 'id',
                               'pw')
                               
TGT_con = python_db_connection('db종류=oracle', 'DB명칭', 'host', 'port', 'id',
                               'pw', oracle_install_space)

## 2. 검증하고자 하는 쿼리 생성하기
SRC_db_name, TGT_db_name = 'db2'.upper(), 'oracle'.upper()

SRC_SCHEMA, TGT_SCHEMA = 'SRC schema 이름', 'TGT schema 이름'

SRC_table_name, TGT_table_name = 'SRC 테이블 명', 'TGT 테이블 명'

SRC_sql = "SELECT * 시작 쿼리"

TGT_sql = "SELECT * 시작 쿼리"

## 3. 1,2번 객체들을 함수에 넣고 건수 검증 수행
result, total_result = total_merge_check(SRC_db_name,
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
                                         fail_list=['⑦', ' ', '⑹', 'ㅼ', 'ъ'])
