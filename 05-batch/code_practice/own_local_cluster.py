import argparse
from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master('spark://MacbookPro:7077')\
                    .appName('local_cluster_test')\
                    .getOrCreate()

parser = argparse.ArgumentParser()

parser.add_argument('--green_input', required=True)
parser.add_argument('--yellow_input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

green_input = args.green_input
yellow_input = args.yellow_input
output = args.output


df_green = spark.read.option("recursiveFileLookup", "true").parquet(green_input)


df_yellow = spark.read.option("recursiveFileLookup", "true").parquet(yellow_input)


df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')\
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime')\
        .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')




yellow_cols = set(df_yellow.columns)
common_cols = []

for green_col in df_green.columns:
    if green_col in yellow_cols:
        common_cols.append(green_col)



from pyspark.sql import functions as F

df_green_sel = df_green.select(common_cols)\
                        .withColumn('service_type', F.lit('green'))

df_yellow_sel = df_yellow.select(common_cols)\
                        .withColumn('service_type', F.lit('yellow'))


df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupBy('service_type').count().show()


df_trips_data.createOrReplaceTempView('trips_data')


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


df_result.coalesce(1).write.parquet(output, mode='overwrite')
print('successfully created report')



# python own_local_cluster.py \
#         --green_input="./data/pq/green/2020/*/" \
#         --yellow_input="./data/pq/yellow/2020/*/" \
#         --output="./data/report-2020"

# URL="spark://MacbookPro:7077"
# spark-submit \
#   --master="${URL}" \
#   own_local_cluster.py \
#         --green_input "./data/pq/green/2020/*/" \
#         --yellow_input "./data/pq/yellow/2020/*/" \
#         --output "./data/report-2020"

# URL="spark://MacbookPro:7077"
# spark-submit \
#   --master="${URL}" \
#   own_local_cluster.py \
#         --green_input "./data/pq/green/2021/*/" \
#         --yellow_input "./data/pq/yellow/2021/*/" \
#         --output "./data/report-2021"