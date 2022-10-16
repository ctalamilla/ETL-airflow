from sqlalchemy import create_engine
import pandas as pd
import seaborn as sns

engine = create_engine('postgresql://airflow:airflow@postgres:5432/stocks')

consulta = 'select * from stocks_values'
respuesta1= pd.read_sql(consulta, engine)
print(respuesta1.info())

