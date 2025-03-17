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
            PULocationID VARCHAR,
            DOLocationID VARCHAR,
            passenger_count VARCHAR,
            trip_distance VARCHAR,
            tip_amount VARCHAR,
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

def create_csv_sink(t_env):
    # Create a directory for session windows
    import subprocess
    subprocess.run(["mkdir", "-p", "/opt/flink/usrlib/session_output"])

    table_name = "session_sink"
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        PULocationID INT,
        DOLocationID INT,
        trip_count BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/opt/flink/usrlib/session_output',
        'format' = 'csv'
    )
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_final_sink(t_env):
    table_name = "results_sink"
    sink_ddl = f"""
    CREATE TABLE {table_name} (
        PULocationID INT,
        DOLocationID INT,
        max_streak BIGINT
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/opt/flink/usrlib/results_output',
        'format' = 'csv'
    )
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def session_job():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    t_env.get_config().get_configuration().set_string(
        "execution.checkpointing.interval", "10s"
    )

    try:
        source_table = create_taxi_events_source_kafka(t_env)
        session_sink = create_csv_sink(t_env)

        # Use SESSION windows as required in the homework
        sql = f"""
        INSERT INTO {session_sink}
        SELECT
            SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTES) AS window_start,
            SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTES) AS window_end,
            CAST(PULocationID AS INT) AS PULocationID,
            CAST(DOLocationID AS INT) AS DOLocationID,
            COUNT(*) AS trip_count
        FROM {source_table}
        GROUP BY 
            CAST(PULocationID AS INT),
            CAST(DOLocationID AS INT),
            SESSION(dropoff_timestamp, INTERVAL '5' MINUTES)
        """

        print("Executing SESSION window aggregation...")
        t_env.execute_sql(sql).wait()

        # Now compute max streaks per location pair
        session_table = t_env.from_path(session_sink)
        t_env.create_temporary_view("session_windows", session_table)

        results_sink = create_final_sink(t_env)
        max_streak_sql = f"""
        INSERT INTO {results_sink}
        SELECT 
            PULocationID,
            DOLocationID,
            MAX(trip_count) AS max_streak
        FROM session_windows
        GROUP BY
            PULocationID,
            DOLocationID
        """

        print("Calculating max streaks...")
        t_env.execute_sql(max_streak_sql).wait()

    except Exception as e:
        print(f"Processing failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    session_job()