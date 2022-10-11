-- create table sql
CREATE TABLE IF NOT EXISTS stocks_daily (
    date TEXT,
    symbol TEXT,
    UNIQUE(date)
);