from pyflink.table import EnvironmentSettings, TableEnvironment
import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
def create_taxi_events_source_kafka(t_env):
    table_name = "green_trips"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def create_output_table(t_env):
    table_name = "session_results"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            max_streak BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def session_job():
    # Use TableEnvironment instead of StreamTableEnvironment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # Set checkpointing and parallelism via configuration
    t_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "10s"
    )
    t_env.get_config().get_configuration().set_string(
        "parallelism.default", "3"
    )

    try:
        # Create the Kafka source table
        source_table = create_taxi_events_source_kafka(t_env)

        # Create a sink table for results
        sink_table = create_output_table(t_env)

        # Use SQL for session window (instead of the Table API)
        result_sql = f"""
        INSERT INTO {sink_table}
        SELECT 
            PULocationID,
            DOLocationID,
            MAX(trip_count) AS max_streak
        FROM (
            SELECT
                PULocationID,
                DOLocationID,
                COUNT(*) AS trip_count,
                SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTES) AS window_start,
                SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTES) AS window_end
            FROM {source_table}
            GROUP BY 
                PULocationID,
                DOLocationID,
                SESSION(dropoff_timestamp, INTERVAL '5' MINUTES)
        )
        GROUP BY
            PULocationID,
            DOLocationID
        """

        # Execute SQL
        print("Executing session window job...")
        t_env.execute_sql(result_sql).wait()
        print("Job completed successfully!")

    except Exception as e:
        print("Processing failed:", str(e))

if __name__ == '__main__':
    session_job()