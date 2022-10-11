import pandas as pd
import numpy as np
import requests


#url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=GOOG&interval=5min&apikey=3B3IE0VNJFZBBMRY'
APIKEYS = ['7C5G34M2E03NDYL3', 'P7TV6UJVGDYPDTUR', 'TFHNYCWBD71JBSON']


BASE_URL = 'https://www.alphavantage.co/query'
API_KEY = '7C5G34M2E03NDYL3'
STOCK_FN = 'TIME_SERIES_INTRADAY'
INTERVAL = '5min'

stock_symbol = {'google': 'GOOG', 'microsoft': 'MSFT', 'amazon': 'AMZN'}


def ingest_data_(company):
    date = '{{ds}}'
    end_point = (f"{BASE_URL}?function={STOCK_FN}&symbol={stock_symbol.get(company)}&interval={INTERVAL}"f"&apikey={np.random.choice(APIKEYS, 1)[0]}&datatype=json")
    print(f"Getting data from {end_point}...")
    data = requests.get(end_point).json()
    print(data)
    print(f"Number of data consulted {len(data.get('Time Series (5min)'))}")
    df = pd.DataFrame(data.get('Time Series (5min)')).T.reset_index().rename(columns={'index':'fecha', '1. open': 'v_open', '2. high': 'v_high','3. low':'v_low', '4. close': 'v_close', '5. volume': 'volume'})
    df['symbol'] = stock_symbol.get(company)
    print(f'Number of data Extracted   {df.shape[0]}')
    df.to_json(f'datalake/rawfile-{stock_symbol.get(company)}.json')
#-{df.loc[0,["fecha"]].datetime[0:10]}