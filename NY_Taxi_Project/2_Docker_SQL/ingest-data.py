#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
import os
from sqlalchemy import create_engine

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file = 'output.parquet'
    #parquet_file = 'yellow_tripdata_2022-01.parquet'
    os.system(f"wget {url} -O {parquet_file}")

    df = pd.read_parquet(f'{parquet_file}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #print(pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con = engine))

    df.head(n=0).to_sql(name = table_name, con = engine, if_exists='replace') #creating table and loading column names

    df.to_sql(name = f'{table_name}', con = engine, if_exists='append') #loading data

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingest parquet data to postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for the postgres')
    parser.add_argument('--host', help='host for the postgres')
    parser.add_argument('--port', help='port for the postgres')
    parser.add_argument('--db', help='db for the postgres')
    parser.add_argument('--table_name', help='name of the table which will store the data')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()
    main(args)
