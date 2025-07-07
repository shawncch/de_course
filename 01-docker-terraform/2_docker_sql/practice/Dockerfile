FROM python:3.9

RUN apt-get install wget
RUN apt-get install gzip
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data_2019.py ingest_data_2019.py  

ENTRYPOINT ["python", "ingest_data_2019.py"]