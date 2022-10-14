#!/usr/bin/python3
from configparser import ConfigParser
from datetime import datetime

import os
import json
import sqlparse


import pandas as pd
import numpy as np

import connection
import conn_warehouse

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")

    #connect to db warehouse
    conn_dwh, engine_dwh  = conn_warehouse.conn()
    cursor_dwh = conn_dwh.cursor()

    #connect to db source
    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    #connect spark
    conf = connection.config('spark')
    spark = connection.spark_conn(app="etldigitalskola",config=conf)
    
    #query extract db source 
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            path_query+'query1.sql','r'
            ).read(), strip_comments=True).strip()
    
    #query load db warehouse
    query_dwh = sqlparse.format(
        open(
            path_query+'dwh.sql','r'
            ).read(), strip_comments=True).strip()
    print(f'[INFO] success create table...')

    #transform etl
    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)
        # print(df)

        #upload local
        filetime = datetime.now().strftime('%Y%m%d')
        path = os.getcwd()
        directory = path+'/'+'local'+'/'
        if not os.path.exists(directory):
            os.makedirs(directory)
        df.to_csv(f"{directory}dim_transaction_{filetime}.csv", index=False)
        print(f"[INFO] Upload Data in LOCAL Success .....")

        #insert dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()
        
        df.to_sql('dim_transaction1', engine_dwh, if_exists='replace', index=False)
        print(f"[INFO] Update DWH Success .....")
       
        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")

