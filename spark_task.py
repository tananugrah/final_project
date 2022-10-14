#!python3

from doctest import DocFileSuite
import pandas as pd
import numpy as np
import os
import sqlparse 
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark import SparkContext

import connection
import conn_warehouse
import app

if __name__ == '__main__':
    print(f"[INFO] Service ETL is Starting .....")


#connect to db source
conf = connection.config('postgresql')
conn, engine = connection.psql_conn(conf)
cursor = conn.cursor()

#connect to db warehouse
conn_dwh, engine_dwh  = conn_warehouse.conn()
cursor_dwh = conn_dwh.cursor()

conf = connection.config('spark')
spark = connection.spark_conn(app="etldigitalskola",config=conf)

path_query = os.getcwd()+'/query/'
query = sqlparse.format(
        open(
            path_query+'query1.sql','r'
            ).read(), strip_comments=True).strip()

df = pd.read_sql(query, engine)
# print(df)

df['birthdate_customer'] = pd.to_datetime(df['birthdate_customer']) 
df['date_transaction'] = pd.to_datetime(df['date_transaction']) 

df['birthdate_customer'] = df['birthdate_customer'].where(df['birthdate_customer']\
     < df['date_transaction'], df['birthdate_customer'] -  np.timedelta64(100, 'Y')) 
df['age'] = (df['date_transaction'] - df['birthdate_customer']).astype('<m8[Y]')
print(df)

path = os.getcwd()
dir = path+ '/' + 'spark_transform'+'/'
SparkDF = spark.createDataFrame(df)

SparkDF.groupBy("product_transaction").sum("amount_transaction").toPandas()\
                .to_csv(f"{dir}product.csv", index=False)
print(f"[INFO] File 1 Sukses")

SparkDF.groupBy("country_customer").count().orderBy('country_customer').toPandas()\
                .to_csv(f"{dir}country.csv", index=False)
print(f"[INFO] File 2 Sukses")

SparkDF.groupBy("age","gender_customer").count().orderBy('age', ascending=True).toPandas()\
                .to_csv(f"{dir}age.csv", index=False)
print(f"[INFO] File 3 Sukses")

    
    

