import os
import gzip
import pandas as pd
from sqlalchemy import create_engine

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "ny_taxi",
    "user": "postgres",
    "password": "postgres",
}

# URLs for datasets
DATA_FILES = [
    ("green_tripdata_2019-10.csv.gz", "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"),
    ("taxi_zone_lookup.csv", "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"),
]

def download_and_extract():
    """Download and extract data files."""
    for filename, url in DATA_FILES:
        if not os.path.exists(filename):
            print(f"Downloading {filename}...")
            os.system(f"wget {url} -O {filename}")
        if filename.endswith(".gz"):
            with gzip.open(filename, "rb") as f_in:
                with open(filename.replace(".gz", ""), "wb") as f_out:
                    f_out.write(f_in.read())
            print(f"Extracted {filename}.")

def load_to_postgres():
    """Load datasets into PostgreSQL."""
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    conn = engine.connect()

    # Load green taxi trips
    green_taxi_df = pd.read_csv("green_tripdata_2019-10.csv")
    green_taxi_df.to_sql("green_taxi_trips", engine, if_exists="replace", index=False)

    # Load taxi zones
    zones_df = pd.read_csv("taxi_zone_lookup.csv")
    zones_df.to_sql("taxi_zones", engine, if_exists="replace", index=False)

    print("Data loaded successfully!")
    conn.close()

if __name__ == "__main__":
    download_and_extract()
    load_to_postgres()
