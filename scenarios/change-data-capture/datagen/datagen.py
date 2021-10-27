import os

wait_for = int(os.environ['wait_for']) if 'wait_for' in os.environ else 30

print(f"Wait {wait_for} seconds.")

import time

time.sleep(wait_for)  # wait for pravega and mysql to start

########################################################################

print("Create scope and stream for pravega.")

import pravega_client

manager = pravega_client.StreamManager("pravega:9090")
manager.create_scope('stock')
manager.create_stream('stock', 'dbserver1', 1)
manager.create_stream('stock', 'dbserver1.stock.stock', 1)
manager.create_stream('stock', 'dbserver1.stock.metadata', 1)

########################################################################

print("Get stock data.")

import yfinance as yf

tickers_list = ['AAPL', 'IBM', 'MU', 'BA', 'TSLA', 'NKE', 'GE', 'MMM']
df = yf.download(tickers=tickers_list, period="5d", interval="1m")

########################################################################

print("Update mysql.")

import mysql.connector

cnx = mysql.connector.connect(user='root', password='dbz',
                              host='mysql',
                              database='stock',
                              autocommit=True)
cursor = cnx.cursor()

import pandas as pd

try:
    for index, sampling_point in df.iterrows():
        for id in tickers_list:
            value = sampling_point['Close'][id]
            if not pd.isna(value):
                cursor.execute(f'INSERT INTO stock (id, value) VALUES("{id}", {value}) ON DUPLICATE KEY UPDATE value = {value};')
        print(f"Update {tickers_list} with {[sampling_point['Close'][id] for id in tickers_list]}")
        time.sleep(1)
except Exception as e:
    print(e)
finally:
    cursor.close()
    cnx.close()
