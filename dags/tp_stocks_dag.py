from asyncore import read
from contextlib import ContextDecorator
import json
from datetime import datetime
from time import sleep

from datetime import datetime as dt

##import sqlalchemy.exc
import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from modules.create_table import create_table as db_create_table

from modules.ingest_data import ingest_data_
from modules.ingest_data import stock_symbol
from modules.read_load import readLoad

dag = DAG(
    dag_id="etl_stocks_final",
    description="descarga de datos",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
)

create_table = PythonOperator(
    task_id="create_table", python_callable=db_create_table, dag=dag
)


ingest_data_task = {}
for company, symbol in stock_symbol.items():
    ingest_data_task[company] = PythonOperator(
        task_id=f"ingest_data_from_{company}",
        python_callable=ingest_data_,
        op_args=[company],
        dag=dag,
    )


load_data = PythonOperator(task_id="load_data_db", python_callable=readLoad, dag=dag)

t1 = DummyOperator(task_id="completed_load", dag=dag)

from modules.report import reporting

make_report = PythonOperator(
    task_id="make_report",
    python_callable=reporting,
    # op_args = stock_symbol,
    dag=dag,
)


for company in stock_symbol:
    upstream_task = create_table
    task = ingest_data_task[company]
    upstream_task.set_downstream(task)
    task.set_downstream(load_data)
    load_data.set_downstream(t1)

t1.set_downstream(make_report)
