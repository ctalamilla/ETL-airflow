"""Stocks dag."""

import json
from datetime import datetime

import pandas as pd
import requests  # type: ignore
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from sqlite_cli import SqLiteClient

BASE_URL = "https://www.alphavantage.co/query"
API_KEY = "TFHNYCWBD71JBSON"
STOCK_FN = "TIME_SERIES_DAILY"

SQL_DB = "/tmp/sqlite_default.db"  # This can be defined in Admin/Connections
SQL_TABLE = "stocks_daily"
SQL_CREATE = f"""
CREATE TABLE IF NOT EXISTS {SQL_TABLE} (
date TEXT,
symbol TEXT,
avg_num_trades REAL,
avg_price REAL,
UNIQUE(date,symbol)
)
"""


def _create_table_if_not_exists(**context):  # pylint:disable=unused-argument
    sql_cli = SqLiteClient(SQL_DB)
    sql_cli.execute(SQL_CREATE)


def _get_stock_data(stock_symbol, **context):
    date = context["ds"]  # read execution date from context
    end_point = (
        f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol}"
        f"&apikey={API_KEY}&datatype=json"
    )
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    data = json.loads(r.content)["Time Series (Daily)"]
    if date in data:
        day_data = data[date]
        avg_price = (float(day_data["2. high"]) + float(day_data["3. low"])) / 2
        avg_num_trades = int(day_data["5. volume"]) / 1440
    else:
        avg_price = avg_num_trades = float("nan")
    return [date, stock_symbol, avg_price, avg_num_trades]


def _insert_daily_data(**context):
    task_instance = context["ti"]
    data = [task_instance.xcom_pull(task_ids="get_daily_data")]
    df = pd.DataFrame(data, columns=["date", "symbol", "avg_price", "avg_num_trades"])
    sql_cli = SqLiteClient(SQL_DB)
    sql_cli.insert_from_frame(df, SQL_TABLE)


default_args = {"owner": "pedro", "retries": 0, "start_date": datetime(2022, 8, 25)}
with DAG("stocks", default_args=default_args, schedule_interval="0 4 * * *") as dag:
    create_table_if_not_exists = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=_create_table_if_not_exists,
    )
    get_daily_data = PythonOperator(
        task_id="get_daily_data", python_callable=_get_stock_data, op_args=["aapl"]
    )
    # Add insert stock data
    insert_daily_data = PythonOperator(
        task_id="insert_daily_data", python_callable=_insert_daily_data
    )
    create_table_if_not_exists >> get_daily_data >> insert_daily_data
