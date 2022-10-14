#!python3

import os
import json
import pandas

from kafka import KafkaConsumer
from sqlalchemy import create_engine, false


if __name__ == "__main__":
    print("starting the consumer")

    #connect database
    try:
        engine = create_engine('postgresql://postgres:postgres@192.168.64.15:5432/digitalskola_dwh')
        print(f"[INFO] Successfully Connect Database .....")
    except:
        print(f"[INFO] Error Connect Database .....")

    #connect kafka server
    try:
        consumer = KafkaConsumer("final-project", bootstrap_servers='localhost')
        print(f"[INFO] Successfully Connect Kafka Server .....")
    except:
        print(f"[INFO] Error Connect Kafka Server .....")

    #read message from topic kafka server
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")
        
    
        #insert database   
        df = pandas.DataFrame(data, index=[0])
        df = df.groupby('date_search[0:7]').count()
        print(df)
        df.to_sql('search_log', engine, if_exists='append', index=False)

        
     