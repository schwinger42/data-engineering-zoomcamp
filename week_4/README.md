# Running the Python Script to Download Data
### Download Data: The Python script provided (`load_yellow_taxi_data.py`) will automate the process of downloading the Parquet files for the months of January 2024 through June 2024.
1. Save the Python Script:
Place the script in your directory and ensure that BUCKET_NAME in the script is set to the name of your GCS bucket.

```Python
BUCKET_NAME = "your_gcs_bucket_name"  # Set this to your bucket name
```
2. **Run the Python Script**: Ensure that the Google Cloud credentials are configured and run the script using Python:
```Python
python3 load_yellow_taxi_data.py 
```

# BigQuery Table Setup
### Create an External Table in BigQuery:
This will allow you to query the Parquet files directly from GCS without actually loading the data into BigQuery storage.
```sql
CREATE OR REPLACE EXTERNAL TABLE `your_project.your_dataset.external_yellow_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your_gcs_bucket_name/yellow/yellow_tripdata_2024-*.parquet']
);
```

If the dataset yellow_taxi_data does not exist, you can create it using the following command:
```bash
bq mk --dataset --location=US your_project_name:yellow_taxi_data
```

### Create the External Table (After Dataset is Created):
Once the dataset is created, you can then run the CREATE EXTERNAL TABLE statement again:
```sql
CREATE OR REPLACE EXTERNAL TABLE `your_project_name.yellow_taxi_data.external_yellow_trips`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your_bucket_name/yellow_tripdata_2024-*.parquet']
);

```