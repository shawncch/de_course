from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import os
from datetime import datetime

from ingest_data import ingest_callable

PATH_TO_LOCAL_HOME = os.environ.get('AIRFLOW_HOME')


yymm = "{{ logical_date.strftime(\'%Y-%m\')}}"
csv_name = f"yellow_tripdata_{yymm}.csv.gz"
csv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{csv_name}"


PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT")
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_TABLENAME = "yellow_taxi_{{ logical_date.strftime(\'%Y_%m\')}}"
PG_CSVNAME = f"{PATH_TO_LOCAL_HOME}/yellow_tripdata_{yymm}.csv"

yymm = "{{ logical_date.strftime(\'%Y-%m\')}}"
csv_name = f"yellow_tripdata_{yymm}.csv.gz"
csv_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{csv_name}"

with DAG(
    dag_id = "local_ingestion_pg",
    schedule = "0 6 2 * *",
    start_date = datetime(2021, 1, 1),
    max_active_runs = 3,
    catchup = True
):
    wget = BashOperator(
        task_id = "wget_trips_csvgz",
        bash_command = f"wget {csv_url} -O {PATH_TO_LOCAL_HOME}/{csv_name} && gunzip -f {PATH_TO_LOCAL_HOME}/{csv_name} || rm -f {PATH_TO_LOCAL_HOME}/{csv_name}"
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
            table_name = PG_TABLENAME,
            trips_csv_name = PG_CSVNAME
        )
    )

    remove = BashOperator(
        task_id = "remove_gz_csv",
        bash_command = f"rm {PATH_TO_LOCAL_HOME}/{csv_name[:-3]}"
    )

    wget >> ingest >> remove