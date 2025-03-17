import csv
import json
import time
from kafka import KafkaProducer
from datetime import datetime

# Function to serialize dictionary to JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Function to filter only the required columns and ensure datetime format
def filter_columns(row, columns):
    filtered_row = {col: row[col] for col in columns if col in row}

    # Ensure datetime columns are in the expected format
    for dt_col in ['lpep_pickup_datetime', 'lpep_dropoff_datetime']:
        if dt_col in filtered_row:
            # Parse datetime from source format and reformat
            try:
                dt_obj = datetime.strptime(filtered_row[dt_col], '%Y-%m-%d %H:%M:%S')
                filtered_row[dt_col] = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                # If parsing fails, keep original value
                pass

    # Convert numeric columns
    for num_col in ['PULocationID', 'DOLocationID', 'passenger_count']:
        if num_col in filtered_row:
            try:
                filtered_row[num_col] = int(filtered_row[num_col])
            except (ValueError, TypeError):
                filtered_row[num_col] = 0

    for float_col in ['trip_distance', 'tip_amount']:
        if float_col in filtered_row:
            try:
                filtered_row[float_col] = float(filtered_row[float_col])
            except (ValueError, TypeError):
                filtered_row[float_col] = 0.0

    return filtered_row

if __name__ == "__main__":
    # Kafka configuration
    server = 'redpanda-1:29092'
    topic_name = 'green-trips'
    producer = KafkaProducer(
        bootstrap_servers=[server],
        value_serializer=json_serializer
    )

    # Path to the dataset
    csv_file = '/opt/src/producers/data/green_tripdata_2019-10.csv'

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
    count = 0

    # Read and process the CSV file
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Filter the row to keep only required columns
                message = filter_columns(row, required_columns)
                # Send the message to Kafka
                producer.send(topic_name, value=message)
                count += 1
                # Print progress every 1000 records
                if count % 1000 == 0:
                    print(f"Processed {count} records")
    except Exception as e:
        print(f"Error processing CSV: {e}")

    # Flush the producer to ensure all messages are sent
    producer.flush()

    # End timing
    t1 = time.time()
    took = t1 - t0
    print(f"Total time to send {count} records and flush: {took:.2f} seconds")