import csv
import json
import time
from kafka import KafkaProducer

# Function to serialize dictionary to JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Function to filter only the required columns
def filter_columns(row, columns):
    return {col: row[col] for col in columns if col in row}

if __name__ == "__main__":
    # Kafka configuration
    server = 'redpanda-1:29092'  # Adjust if running outside Docker
    topic_name = 'green-trips'
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer
    )

    # Path to the dataset
    csv_file = '/opt/src/producers/data/green_tripdata_2019-10.csv'  # Adjust path as needed

    # Columns to keep
    required_columns = [
        'lpep_pickup_datetime',
        'lpep_dropoff_datetime',
        'PULocationID',
        'DOLocationID',
        'passenger_count',
        'trip_distance',
        'tip_amount'
    ]

    # Start timing
    t0 = time.time()

    # Read and process the CSV file
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Filter the row to keep only required columns
            message = filter_columns(row, required_columns)
            # Send the message to Kafka
            producer.send(topic_name, value=message)

    # Flush the producer to ensure all messages are sent
    producer.flush()

    # End timing
    t1 = time.time()
    took = t1 - t0
    print(f"Total time to send and flush: {took:.2f} seconds")