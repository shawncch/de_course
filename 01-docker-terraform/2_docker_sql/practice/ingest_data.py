#!/usr/bin/env python
# coding: utf-8

import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    database_name = args.database_name
    table_name = args.table_name
    trips_url = args.trips_url
    zones_url = args.zones_url

    trips_csv_name = 'output.csv'
    zones_csv_name = 'zones.csv'

    os.system(f"wget {trips_url} -O {trips_csv_name}.gz")
    print("successfully downloaded the file")
    
    if os.path.exists(trips_csv_name):
        os.remove(trips_csv_name)
    
    os.system(f"gunzip {trips_csv_name}.gz")
    print("successfully unzipped the file")

    os.system(f"wget {zones_url} -O {zones_csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')

    df = pd.read_csv(trips_csv_name, nrows= 100)

    # convert to datetime
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    # create or replace table
    df.head(0).to_sql(name = table_name, con=engine, if_exists= "replace")

    # iterator to read the file in chunks
    df_iter = pd.read_csv(trips_csv_name, iterator=True, chunksize = 100000)


    # trips ingestion
    zones_df = pd.read_csv(zones_csv_name)

    zones_df.to_sql("zones", con=engine, if_exists="replace")
    print("created zones static table and ingested data")

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                        description='Ingest CSV data to Postgres',
                    )

    # user
    # password
    # host 
    # port
    # database name 
    # table name 
    # url of csv

    parser.add_argument('--user', help = 'user name for postgres')
    parser.add_argument('--password', help = 'password for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'port for postgres')
    parser.add_argument('--database_name', help = 'database name for postgres')
    parser.add_argument('--table_name', help = 'name of table where we will write the results to')
    parser.add_argument('--trips_url', help = 'url of trips csv')
    parser.add_argument('--zones_url', help = 'url of zones csv')

    args = parser.parse_args()

    main(args)




#to drop table

# from sqlalchemy import text
# with engine.begin() as conn:
#     conn.execute(text("drop table ny_taxi1"))


# pd.read_sql(con=engine, sql = "select 1 as no")

# print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))