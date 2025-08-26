

from pyflink.table import EnvironmentSettings, TableEnvironment
import os
import json
import pyflink
from datetime import datetime, timezone


#######################################
# 1. Creates the execution environment
#######################################

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)



if is_local:
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///" + CURRENT_DIR + "/target/pyflink-dependencies.jar",
    )

    print("PyFlink home: " + os.path.dirname(os.path.abspath(pyflink.__file__)))
    print("Logging directory: " + os.path.dirname(os.path.abspath(pyflink.__file__)) + '/log')

def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))

def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


AT_TIMESTAMP = "AT_TIMESTAMP"

def main():


    props = get_application_properties()

    # Input stream configuration
    input_stream_properties = property_map(props, "InputStream0")
    input_stream_arn = input_stream_properties["stream.arn"]
    input_stream_region = input_stream_properties["aws.region"]
    input_stream_init_position = input_stream_properties["flink.source.init.position"] if "flink.source.init.position" in input_stream_properties else None
    input_stream_init_timestamp = input_stream_properties["flink.source.init.timestamp"] if "flink.source.init.timestamp" in input_stream_properties else None
    if input_stream_init_position == AT_TIMESTAMP and input_stream_init_timestamp == None:
            raise ValueError(f"A timestamp must be supplied for flink.source.init.position = {AT_TIMESTAMP}")



    source_init_pos = "\n'source.init.position' = '{0}',".format(
        input_stream_init_position) if input_stream_init_position is not None else ''
    source_init_timestamp = "\n'source.init.timestamp' = '{0}',".format(
        input_stream_init_timestamp) if input_stream_init_timestamp is not None else ''

    # 1️⃣ Source table (Kinesis)
    table_env.execute_sql(f"""
        CREATE TABLE temperature_metrics (
                city VARCHAR(10),
                temperature NUMERIC(6,2),
                updated_at TIMESTAMP_LTZ(3)
              )
              PARTITIONED BY (city)
              WITH (
                'connector' = 'kinesis',
                'stream.arn' = '{input_stream_arn}',
                'aws.region' = '{input_stream_region}',
                {source_init_pos}{source_init_timestamp}
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601',
                'json.ignore-parse-errors' = 'true',       -- skip malformed JSON
                'json.fail-on-missing-field' = 'false'     -- allow missing fields
              )
    """)

    # 2️⃣ Main sink for valid data
    table_env.execute_sql("""
        CREATE TABLE output (
            city VARCHAR(10),
            temperature NUMERIC(6,2),
            updated_at TIMESTAMP_LTZ(3)
        )
        WITH (
            'connector' = 'print',
            'print-identifier' = 'PROCESSED_ROW'
        )
    """)

    # 3️⃣ Sink for invalid/malformed rows
    table_env.execute_sql("""
        CREATE TABLE bad_rows (
            city VARCHAR(10),
            temperature NUMERIC(6,2),
            updated_at TIMESTAMP_LTZ(3)
        )
        WITH (
            'connector' = 'print',
            'print-identifier' = 'BAD_ROW'
        )
    """)

    # 4️⃣ Insert valid rows into main sink
    table_result = table_env.execute_sql("""
        INSERT INTO output
        SELECT city, temperature, updated_at
        FROM temperature_metrics
        WHERE city IS NOT NULL
          AND temperature IS NOT NULL
          AND updated_at IS NOT NULL
    """)

    # 5️⃣ Insert invalid rows into bad_rows sink
    table_env.execute_sql("""
        INSERT INTO bad_rows
        SELECT city, temperature, updated_at
        FROM temperature_metrics
        WHERE city IS NULL
           OR temperature IS NULL
           OR updated_at IS NULL
    """)

    if is_local:
        table_result.wait()


if __name__ == "__main__":
    main()
