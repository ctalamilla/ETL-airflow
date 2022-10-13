import sys; sys.path.insert(1, '/opt/airflow/dags')
import pytest
from modules.report import query


#print(query('GOOG')['symbol'].unique()[0])

def test_db_query():
    assert query('GOOG')['symbol'].unique()[0] == 'GOOG'
    assert query('AMZN')['symbol'].unique()[0] == 'AMZN'
    assert query('MSFT')['symbol'].unique()[0] == 'MSFT'
