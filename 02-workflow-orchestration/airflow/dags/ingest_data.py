import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

def ingest_callable(user, password, host, port, database_name, table_name, trips_csv_name):

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')

    df = pd.read_csv(trips_csv_name, nrows= 100)

    # convert to datetime
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    # create or replace table
    df.head(0).to_sql(name = table_name, con=engine, if_exists= "replace")

    # iterator to read the file in chunks
    df_iter = pd.read_csv(trips_csv_name, iterator=True, chunksize = 100000)

    # batch ingestion
    while True:
        try:

            start_time = time()

            df = next(df_iter)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

            df.to_sql(name=table_name, con=engine, if_exists = "append")

            end_time = time()

            print("inserted 100000 chunks, took %.3f" % (end_time-start_time)) 

        except StopIteration:
            break
