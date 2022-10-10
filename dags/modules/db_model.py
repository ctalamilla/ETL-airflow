
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


from datetime import datetime
from sqlalchemy import Column, Float, String, Integer, DateTime
from datetime import datetime


Base = declarative_base()

class StocksValues(Base):
    """ Stock data model """
    __tablename__ = 'stocks_values'
    id = Column(Integer, primary_key = True )
    date = Column(DateTime)
    v_open = Column(Float)
    v_high = Column(Float)
    v_low = Column(Float)
    v_close = Column(Float)
    volume = Column(Integer)
    symbol = Column(String(10))
    created_at = Column(DateTime, default = datetime.now())
    
    def __repr__(self):
        return f"<StockValue(symbol='{self.symbol}', ...)>"


    



