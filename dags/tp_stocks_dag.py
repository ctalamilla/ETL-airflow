from contextlib import ContextDecorator
import json
from datetime import datetime
from time import sleep

import pandas as pd
import numpy as np
import requests  # type: ignore
from datetime import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float
##import sqlalchemy.exc
import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from modules.create_table import create_table as db_create_table


#from airflow.providers.sqlite.operators.sqlite import SqliteOperator
#from sqlite_cli import SqLiteClient

#url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=GOOG&interval=5min&apikey=3B3IE0VNJFZBBMRY'
APIKEYS = ['7C5G34M2E03NDYL3', 'P7TV6UJVGDYPDTUR', 'TFHNYCWBD71JBSON']


BASE_URL = 'https://www.alphavantage.co/query'
API_KEY = '7C5G34M2E03NDYL3'
STOCK_FN = 'TIME_SERIES_INTRADAY'
INTERVAL = '5min'

stock_symbol = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}


def ingest_data(company):
    date = '{{ds}}'
    end_point = (f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol.get(company)}&interval={INTERVAL}"f"&apikey={np.random.choice(APIKEYS, 1)[0]}&datatype=json")
    print(f"Getting data from {end_point}...")
    data = requests.get(end_point).json()
    print(data)
    print(f"Number of data consulted {len(data.get('Time Series (5min)'))}")
    df = pd.DataFrame(data.get('Time Series (5min)')).T.reset_index().rename(columns={'index':'datetime', '1. open': 'open', '2. high': 'high','3. low':'low', '4. close': 'close', '5. volume': 'volume'})
    df['symbol'] = stock_symbol.get(company)
    print(f'Number of data Extracted   {df.shape[0]}')
    df.to_json(f'datalake/rawfile-{stock_symbol.get(company)}-{df.loc[0,["datetime"]].datetime[0:10]}.json')
#df.loc[0,["datetime"]].datetime[0:10]}



dag = DAG(
    dag_id="etl_stocks",
    description="descarga de datos",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@daily",
)

create_table = PythonOperator(task_id = "create_table",
                    python_callable= db_create_table,
                    dag = dag)


t1 = DummyOperator(task_id="hello-world2", dag= dag)

ingest_data_task= {}
for company, symbol in stock_symbol.items():
    ingest_data_task[company] = PythonOperator(
        task_id = f'ingest_data_from_{company}',
        python_callable = ingest_data,
        op_args= [company],
        dag=dag
    )

for company in stock_symbol:
    upstream_task = create_table
    task = ingest_data_task[company]
    upstream_task.set_downstream(task)
    task.set_downstream(t1)

    

##t0 >> ingest_daily_data 


