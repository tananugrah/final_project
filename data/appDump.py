#!python3

import pandas as pd
import numpy as np

from sqlalchemy import create_engine

if __name__ == "__main__":
    username = 'postgres'
    password = 'postgres'
    database = 'digitalskola'
    ip       = '192.168.64.15'

    try:
        engine = create_engine(f'postgresql://{username}:{password}@{ip}:5432/{database}')
        print(f"[INFO] success connect to database....")
    except:
        print(f"[INFO] error to connect database...")

    list_filename = ['customer','product','transaction']
    for file in list_filename:
        pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con = engine)
        print(f"[INFO] success dump file {file} ...")