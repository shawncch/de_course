from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import os
from datetime import datetime

from ingest_data import ingest_callable

PATH_TO_LOCAL_HOME = os.environ.get('AIRFLOW_HOME')
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")

yymm = "{{ logical_date.strftime(\'%Y-%m\')}}"

def local_dag_ingest(dag, csvgz_path, tablename, csv_url):
    with dag:
        wget = BashOperator(
            task_id = "wget_trips_csvgz",
            bash_command = f"wget {csv_url} -O {csvgz_path} && gunzip -f {csvgz_path} || rm -f {csvgz_path}"
        )

        ingest = PythonOperator(
            task_id = "ingest",
            python_callable = ingest_callable,
            op_kwargs = dict(
                user = PG_USER,
                password = PG_PASSWORD,
                host = PG_HOST,
                port = PG_PORT,
                database_name = PG_DATABASE,
                table_name = tablename,
                trips_csv_name = csvgz_path[:-3]
            )
        )

        remove = BashOperator(
            task_id = "remove_gz_csv",
            bash_command = f"rm {csvgz_path[:-3]}"
        )

        wget >> ingest >> remove


yellow_csv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{yymm}.csv.gz"
yellow_tablename = "yellow_taxi_{{ logical_date.strftime(\'%Y_%m\')}}"
yellow_csvgz_path = f"{PATH_TO_LOCAL_HOME}/yellow_tripdata_{yymm}.csv.gz"

yellow_dag = DAG(
    dag_id = "local_ingestion_pg_yellow",
    schedule = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 1, 1),
    max_active_runs = 3,
    catchup = True
)

local_dag_ingest(yellow_dag, yellow_csvgz_path, yellow_tablename, yellow_csv_url)




green_csv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{yymm}.csv.gz"
green_tablename = "green_taxi_{{ logical_date.strftime(\'%Y_%m\')}}"
green_csvgz_path = f"{PATH_TO_LOCAL_HOME}/green_tripdata_{yymm}.csv.gz"

green_dag = DAG(
    dag_id = "local_ingestion_pg_green",
    schedule = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 1, 1),
    max_active_runs = 3,
    catchup = True
)

local_dag_ingest(green_dag, green_csvgz_path, green_tablename, green_csv_url)