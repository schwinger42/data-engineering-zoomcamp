from pyflink.table import EnvironmentSettings, TableEnvironment
import os

os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

def create_session_source(t_env):
    table_name = "session_windows"
    source_ddl = f"""
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
    t_env.execute_sql(source_ddl)
    return table_name

def create_results_sink(t_env):
    # Create directory for results
    import subprocess
    subprocess.run(["mkdir", "-p", "/opt/flink/usrlib/results_output"])

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

def process_results():
    settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(settings)

    try:
        # Set up source and sink
        session_source = create_session_source(t_env)
        results_sink = create_results_sink(t_env)

        # Calculate max streaks per location pair
        max_streak_sql = f"""
        INSERT INTO {results_sink}
        SELECT 
            PULocationID,
            DOLocationID,
            MAX(trip_count) AS max_streak
        FROM {session_source}
        GROUP BY
            PULocationID,
            DOLocationID
        """

        print("Calculating max streaks from session windows...")
        t_env.execute_sql(max_streak_sql).wait()
        print("Processing completed successfully!")

    except Exception as e:
        print(f"Processing failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    process_results()