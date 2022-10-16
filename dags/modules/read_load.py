import re
import pandas as pd
import os
from sqlalchemy import create_engine
from modules.psql_cli import psql_Client


def readLoad():
    # import and to instance psql_Cli
    db = "airflow:airflow@postgres:5432/stocks"
    psql_cli = psql_Client(db)

    listfiles = os.listdir("/opt/airflow/datalake/")
    allfiles = [f"/opt/airflow/datalake/{i}" for i in listfiles]
    print(allfiles)
    # print(f'../../datalake/{listfiles[0]}')
    # df = pd.read_json(allfiles[0])
    df_all = []
    for filename in allfiles:
        df_all.append(pd.read_json(filename))

    # Concatenate all data into one DataFrame
    big_frame = pd.concat(df_all, ignore_index=True)
    # print(big_frame.sample(3))
    # print(big_frame['symbol'].unique())
    psql_cli.insert_from_frame(big_frame, "stocks_values")


if __name__ == "__main__":
    readLoad()

# DROP SCHEMA public CASCADE;
# CREATE SCHEMA public;
# engine = create_engine('postgresql://airflow:airflow@postgres:5432/stocks')
# df.to_sql('stocks_values', engine, if_exists= 'append')
