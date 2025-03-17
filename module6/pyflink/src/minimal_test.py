from pyflink.table import TableEnvironment, EnvironmentSettings

def test_pyflink():
    # Create a pure Table environment (not StreamTableEnvironment)
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    # Create a simple test table
    t_env.execute_sql("""
    CREATE TEMPORARY TABLE test_table (
        id INT,
        name STRING
    ) WITH (
        'connector' = 'datagen',
        'number-of-rows' = '10'
    )
    """)

    # Execute a simple query
    result = t_env.execute_sql("SELECT * FROM test_table")
    print(result.get_job_client().get_job_status())

    # Print results
    print("\nResults:")
    result.print()

if __name__ == '__main__':
    test_pyflink()
