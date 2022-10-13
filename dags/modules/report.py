from sqlalchemy import create_engine
import pandas as pd
import mplfinance as mpf

from modules.ingest_data import stock_symbol

engine = create_engine('postgresql://airflow:airflow@postgres:5432/stocks')

def reporting():
    for company, symbol in stock_symbol.items():

        consulta = f"select * from stocks_values where symbol = '{symbol}'"
        respuesta= pd.read_sql(consulta, engine)
        
        print(f'Consulting data from the database.....rows: {respuesta.shape[0]} from company: {company}')
        query = respuesta.loc[:, ['fecha', 'v_open', 'v_high', 'v_low' ,'v_close', 'volume', 'symbol']].set_index('fecha').sort_index(ascending=True).rename(columns={'v_open': 'Open', 'v_high': 'High','v_low':'Low', 'v_close': 'Close', 'volume': 'Volume'})
        #print(query)
        
        print(f'Drawing report for {company}  -  {symbol}')
        mpf.plot(query, type='candle', mav=(3,6,9), volume=True, savefig= f'/opt/airflow/dags/reports/{company}-{symbol}.png')
