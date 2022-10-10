from sqlalchemy.orm import sessionmaker
from modules.db_model import Base
from sqlalchemy import create_engine

def create_table():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/stocks')
    Session = sessionmaker(bind = engine)
    session = Session()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)