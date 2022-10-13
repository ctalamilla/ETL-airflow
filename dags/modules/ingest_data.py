import pandas as pd
import numpy as np
import requests
from datetime import datetime as dt
from datetime import timedelta

#url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=GOOG&interval=5min&apikey=3B3IE0VNJFZBBMRY'
APIKEYS = ['7C5G34M2E03NDYL3', 'P7TV6UJVGDYPDTUR', 'TFHNYCWBD71JBSON']


BASE_URL = 'https://www.alphavantage.co/query'
API_KEY = '7C5G34M2E03NDYL3'
STOCK_FN = 'TIME_SERIES_INTRADAY'
INTERVAL = '30min'

stock_symbol = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}

def find_data(company):
    end_point = (f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol.get(company)}&interval={INTERVAL}&outputsize=full"f"&apikey={np.random.choice(APIKEYS, 1)[0]}&datatype=json")
    print(f"Getting data from {end_point}...")
    data = requests.get(end_point).json()
    return data



def ingest_data_(company):
    """Function to ingest data from de API and save data into Dalake"""
    
    data = data = find_data(company)
    print(data)
    
    print(f"Number of data consulted {len(data.get('Time Series (30min)'))}")
    
    df = pd.DataFrame(data.get('Time Series (30min)')).T.reset_index().rename(columns={'index':'fecha', '1. open': 'v_open', '2. high': 'v_high','3. low':'v_low', '4. close': 'v_close', '5. volume': 'volume'})
    df['symbol'] = stock_symbol.get(company)
    #df['fecha'] = df['fecha'].apply(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S'))
    df = df.loc[df['fecha'].apply(lambda x: dt.strptime(x, '%Y-%m-%d %H:%M:%S')).apply(lambda x: x>= dt.now() - timedelta(days=7))]
    
    
    print(f'Number of data Extracted   {df.shape[0]}')
    df.to_json(f'datalake/rawfile-{stock_symbol.get(company)}.json')
