#!/bin/bash

#print service time
date

#virtualenv is now active
source /home/final_project/venv/bin/activate

# #running etl service
# python3  /home/final_project/app.py

# filetime=$(date+"%Y%m%d")
# filetime=datetime.strftime('%Y%m%d')
echo "[INFO] Mapreduce is Running ....."
#running mapreduce on local
python3 /home/final_project/mapReduce.py /home/final_project/local/dim_transaction_20221013.csv > /home/final_project/mapreduce_transform/ordertotal_output_local_map.csv
python3 /home/final_project/mapReduce_jumlah_barang.py /home/final_project/local/dim_transaction_20221013.csv > /home/final_project/mapreduce_transform/jumlah_barang.csv
python3 /home/final_project/mapReduce_jumlah_transaksi.py /home/final_project/local/dim_transaction_20221013.csv > /home/final_project/mapreduce_transform/jumlah_transaksi.csv
#running mapreduce hadoop
# python3 /home/final_project/mapReduce.py -r hadoop hdfs:///digitalskola/project/dim_orders_$filetime.csv > /home/hadoop_processing/output/orderctotal_output_hadoop_map.txt

echo "[INFO] Mapreduce is Done ....."