import csv
import json
from kafka import KafkaProducer
import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = '/Users/shawn/Desktop/de_course/06-streaming/pyflink/homework/green_tripdata_2019-10.csv'  # change to your CSV file path if needed

    start_time = time.time()
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            message = {
                'lpep_pickup_datetime' : row['lpep_pickup_datetime'],
                'lpep_dropoff_datetime' : row['lpep_dropoff_datetime'],
                'PULocationID' : row['PULocationID'],
                'DOLocationID' : row['DOLocationID'],
                'passenger_count' : row['passenger_count'],
                'trip_distance' : row['trip_distance'],
                'tip_amount' : row['tip_amount']
            }
            producer.send('green-trips', value=message)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()
    end_time = time.time()
    print(f'took {end_time - start_time} seconds')


if __name__ == "__main__":
    main()