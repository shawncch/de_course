import os
import logging

from airflow import DAG
# from airflow.utils.dates import days_ago
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')




def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)



def download_format_upload_dag(dag, dataset_url, dataset_file, parquet_file, gcs_path):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"echo {dataset_url} && wget --no-verbose {dataset_url} -O {PATH_TO_LOCAL_HOME}/{dataset_file}.gz && gunzip -f {PATH_TO_LOCAL_HOME}/{dataset_file}.gz || echo 'invalid link or file not in gz format' && wget {dataset_url} -O {PATH_TO_LOCAL_HOME}/{dataset_file}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{PATH_TO_LOCAL_HOME}/{dataset_file}",
            },
        )

        # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path,
                "local_file": f"{PATH_TO_LOCAL_HOME}/{parquet_file}",
            },
        )

        remove_task = BashOperator(
            task_id = "remove_csv_pq",
            bash_command = f"rm {PATH_TO_LOCAL_HOME}/{parquet_file} {PATH_TO_LOCAL_HOME}/{dataset_file}"
        )

        # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        #     task_id="bigquery_external_table_task",
        #     table_resource={
        #         "tableReference": {
        #             "projectId": PROJECT_ID,
        #             "datasetId": BIGQUERY_DATASET,
        #             "tableId": "external_table",
        #         },
        #         "externalDataConfiguration": {
        #             "sourceFormat": "PARQUET",
        #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
        #         },
        #     },
        # )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_task
        # download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task



yymm = "{{ logical_date.strftime(\'%Y-%m\') }}"
yyyy = "{{ logical_date.strftime(\'%Y\') }}"


yellow_dataset_file = f"yellow_tripdata_{yymm}.csv"
yellow_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{yellow_dataset_file}.gz"
yellow_parquet_file = yellow_dataset_file.replace('.csv', '.parquet')
yellow_gcs_path = f"raw/yellow_trips/{yyyy}/{yellow_parquet_file}"

yellow_dag = DAG(
    dag_id = "upload_gcs_yellowtrips",
    schedule = "0 6 2 * *",
    catchup = True,
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 1, 1),
    max_active_runs = 3
)

download_format_upload_dag(yellow_dag, yellow_dataset_url, yellow_dataset_file, yellow_parquet_file, yellow_gcs_path)



green_dataset_file = f"green_tripdata_{yymm}.csv"
green_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{green_dataset_file}.gz"
green_parquet_file = green_dataset_file.replace('.csv', '.parquet')
green_gcs_path = f"raw/green_trips/{yyyy}/{green_parquet_file}"

green_dag = DAG(
    dag_id = "upload_gcs_greentrips",
    schedule = "0 6 2 * *",
    catchup = True,
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 1, 1),
    max_active_runs = 3
)

download_format_upload_dag(green_dag, green_dataset_url, green_dataset_file, green_parquet_file, green_gcs_path)





#  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
fhv_dataset_file = f"fhv_tripdata_{yymm}.csv"
fhv_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{fhv_dataset_file}.gz"
fhv_parquet_file = fhv_dataset_file.replace('.csv', '.parquet')
fhv_gcs_path = f"raw/fhv_trips/{yyyy}/{fhv_parquet_file}"

fhv_dag = DAG(
    dag_id = "upload_gcs_fhvtrips",
    schedule = "0 6 2 * *",
    catchup = True,
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2020, 1, 1),
    max_active_runs = 3
)

download_format_upload_dag(fhv_dag, fhv_dataset_url, fhv_dataset_file, fhv_parquet_file, fhv_gcs_path)



# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
zones_dataset_file = "taxi_zone_lookup.csv"
zones_dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
zones_parquet_file = zones_dataset_file.replace('.csv', '.parquet')
zones_gcs_path = f"raw/taxi_zones/{zones_parquet_file}"

zones_dag = DAG(
    dag_id = "upload_gcs_taxizones",
    schedule = "@once",
)

download_format_upload_dag(zones_dag, zones_dataset_url, zones_dataset_file, zones_parquet_file, zones_gcs_path)