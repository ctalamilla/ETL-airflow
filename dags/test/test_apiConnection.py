import sys; sys.path.insert(1, '/opt/airflow/dags')
import pytest
from modules.ingest_data import find_data

response_api_google = find_data('google')
response_api_amazon = find_data('amazon')
response_api_microsoft = find_data('microsoft')

def test_api():
    assert response_api_google['Meta Data']['2. Symbol'] == 'GOOG'
    assert response_api_amazon['Meta Data']['2. Symbol'] == 'AMZN'
    assert response_api_microsoft['Meta Data']['2. Symbol'] == 'MSFT'

