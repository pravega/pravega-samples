import os
import time

mysql_password = os.environ['mysql_password'] if 'mysql_password' in os.environ else 'dbz'

########################################################################

print("Create scope and stream for pravega.")

import pravega_client

manager = pravega_client.StreamManager("tcp://pravega:9090")
manager.create_scope('stock')
manager.create_stream('stock', 'dbserver1', 1)
manager.create_stream('stock', 'dbserver1.stock.stock', 1)
manager.create_stream('stock', 'dbserver1.stock.metadata', 1)

# create a file so that the healthcheck will know pravega is ready
with open('log', 'w') as fp:
    fp.write('Scope and streams are created.')

########################################################################

print("Get stock data.")

import yfinance as yf

tickers_list = ['AAPL', 'IBM', 'MU', 'BA', 'TSLA', 'NKE', 'GE', 'MMM']
df = yf.download(tickers=tickers_list, period="5d", interval="1m")

########################################################################

print("Update MySQL.")

import mysql.connector

cnx = mysql.connector.connect(user='root',
                              password=mysql_password,
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
