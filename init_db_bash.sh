#!/bin/bash

psql -U airflow -f /docker-entrypoint-initdb.d/init_db_stocks.sql