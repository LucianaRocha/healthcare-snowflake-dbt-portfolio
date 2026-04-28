import os
from pathlib import Path

import boto3
import snowflake.connector
from dotenv import load_dotenv


PARTD_FILE_NAME = "partd_prescribers_2023.csv"
PARTD_S3_FOLDER = "partd-prescribers"
PARTD_FILE_FORMAT = "PARTD_CSV_FORMAT"
PARTD_STAGE = "PARTD_S3_STAGE"
PARTD_TABLE = "PARTD_PRESCRIBERS"


def load_config():
    """Load environment variables from .env file."""
    load_dotenv()

    return {
        # AWS
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),

        # Local file folder
        "LOCAL_FILE_PATH": os.getenv("LOCAL_FILE_PATH"),

        # Snowflake
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
    }


def validate_config(config):
    """Ensure all required environment variables exist."""
    missing_vars = [key for key, value in config.items() if not value]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


def validate_local_file(base_folder, file_name):
    """Validate that the selected Part D CSV file exists."""
    path = Path(base_folder) / file_name

    if not path.exists():
        raise FileNotFoundError(f"Local file not found: {path}")

    if path.suffix.lower() != ".csv":
        raise ValueError(f"Expected a CSV file, but got: {path.suffix}")

    file_size_gb = path.stat().st_size / (1024 ** 3)

    print(f"Local file found: {path}")
    print(f"File size: {file_size_gb:.2f} GB")

    return path


def upload_file_to_s3(local_file_path, config):
    """Upload the local Part D CSV file to S3."""
    file_name = local_file_path.name
    s3_key = f"{PARTD_S3_FOLDER}/{file_name}"

    print(f"Uploading file to S3: s3://{config['S3_BUCKET_NAME']}/{s3_key}")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        region_name=config["AWS_REGION"],
    )

    s3_client.upload_file(
        Filename=str(local_file_path),
        Bucket=config["S3_BUCKET_NAME"],
        Key=s3_key,
    )

    print("File uploaded to S3 successfully.")

    return s3_key


def connect_to_snowflake(config):
    """Create Snowflake connection."""
    print("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        account=config["SNOWFLAKE_ACCOUNT"],
        user=config["SNOWFLAKE_USER"],
        password=config["SNOWFLAKE_PASSWORD"],
        warehouse=config["SNOWFLAKE_WAREHOUSE"],
        database=config["SNOWFLAKE_DATABASE"],
        schema=config["SNOWFLAKE_SCHEMA"],
        role=config["SNOWFLAKE_ROLE"],
    )

    print("Connected to Snowflake successfully.")
    return conn


def test_snowflake_connection(conn):
    """Run simple Snowflake connection test."""
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"Snowflake version: {version}")

    finally:
        cursor.close()


def create_snowflake_database_and_schema(conn, config):
    """Create Snowflake database and schema if they do not exist."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE ROLE {config['SNOWFLAKE_ROLE']}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config['SNOWFLAKE_DATABASE']}")
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}"
        )

        print("Snowflake database and schema verified successfully.")

    finally:
        cursor.close()


def set_snowflake_context(conn, config):
    """Set active Snowflake role, warehouse, database, and schema."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE ROLE {config['SNOWFLAKE_ROLE']}")
        cursor.execute(f"USE WAREHOUSE {config['SNOWFLAKE_WAREHOUSE']}")
        cursor.execute(f"USE DATABASE {config['SNOWFLAKE_DATABASE']}")
        cursor.execute(f"USE SCHEMA {config['SNOWFLAKE_SCHEMA']}")

        print("Snowflake context set successfully.")

    finally:
        cursor.close()


def create_snowflake_file_format(conn):
    """Create CSV file format for CMS Part D data."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE OR REPLACE FILE FORMAT {PARTD_FILE_FORMAT}
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        """)

        print(f"Snowflake file format {PARTD_FILE_FORMAT} created successfully.")

    finally:
        cursor.close()


def create_snowflake_stage(conn, config):
    """Create external Snowflake stage pointing to the Part D S3 folder."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE OR REPLACE STAGE {PARTD_STAGE}
            URL = 's3://{config["S3_BUCKET_NAME"]}/{PARTD_S3_FOLDER}/'
            CREDENTIALS = (
                AWS_KEY_ID = '{config["AWS_ACCESS_KEY_ID"]}'
                AWS_SECRET_KEY = '{config["AWS_SECRET_ACCESS_KEY"]}'
            )
            FILE_FORMAT = {PARTD_FILE_FORMAT}
        """)

        print(f"Snowflake external stage {PARTD_STAGE} created successfully.")

    finally:
        cursor.close()


def create_raw_partd_table(conn):
    """Create RAW.PARTD_PRESCRIBERS table with explicit CMS Part D schema."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {PARTD_TABLE} (
                PRSCRBR_NPI STRING,
                PRSCRBR_LAST_ORG_NAME STRING,
                PRSCRBR_FIRST_NAME STRING,
                PRSCRBR_CITY STRING,
                PRSCRBR_STATE_ABRVTN STRING,
                PRSCRBR_STATE_FIPS STRING,
                PRSCRBR_TYPE STRING,
                PRSCRBR_TYPE_SRC STRING,
                BRND_NAME STRING,
                GNRC_NAME STRING,
                TOT_CLMS NUMBER,
                TOT_30DAY_FILLS FLOAT,
                TOT_DAY_SUPLY NUMBER,
                TOT_DRUG_CST FLOAT,
                TOT_BENES NUMBER,
                GE65_SPRSN_FLAG STRING,
                GE65_TOT_CLMS NUMBER,
                GE65_TOT_30DAY_FILLS FLOAT,
                GE65_TOT_DRUG_CST FLOAT,
                GE65_TOT_DAY_SUPLY NUMBER,
                GE65_BENE_SPRSN_FLAG STRING,
                GE65_TOT_BENES NUMBER
            )
        """)

        print(f"RAW table {PARTD_TABLE} verified successfully.")

    finally:
        cursor.close()


def copy_into_partd_table(conn):
    """Load staged CMS Part D CSV data into RAW.PARTD_PRESCRIBERS."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            COPY INTO {PARTD_TABLE}
            FROM (
                SELECT
                    $1::STRING,
                    $2::STRING,
                    $3::STRING,
                    $4::STRING,
                    $5::STRING,
                    $6::STRING,
                    $7::STRING,
                    $8::STRING,
                    $9::STRING,
                    $10::STRING,
                    $11::NUMBER,
                    $12::FLOAT,
                    $13::NUMBER,
                    $14::FLOAT,
                    $15::NUMBER,
                    $16::STRING,
                    $17::NUMBER,
                    $18::FLOAT,
                    $19::FLOAT,
                    $20::NUMBER,
                    $21::STRING,
                    $22::NUMBER
                FROM @{PARTD_STAGE}
            )
            FILE_FORMAT = (FORMAT_NAME = {PARTD_FILE_FORMAT})
            ON_ERROR = CONTINUE
        """)

        print(f"COPY INTO {PARTD_TABLE} completed successfully.")

    finally:
        cursor.close()


def validate_partd_row_count(conn):
    """Validate row count after loading Part D data."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"SELECT COUNT(*) FROM {PARTD_TABLE}")
        row_count = cursor.fetchone()[0]

        print(f"{PARTD_TABLE} row count: {row_count:,}")

    finally:
        cursor.close()


def main():
    """Main Part D ingestion pipeline entrypoint."""
    print("Starting Medicare Part D ingestion script...")

    RUN_UPLOAD_TO_S3 = False
    RUN_CREATE_INFRASTRUCTURE = False
    RUN_SNOWFLAKE_TEST = True
    RUN_CREATE_STAGE = False
    RUN_CREATE_RAW_TABLE = False
    RUN_COPY_INTO = False

    config = load_config()
    validate_config(config)

    local_file_path = validate_local_file(
        config["LOCAL_FILE_PATH"],
        PARTD_FILE_NAME
    )

    if RUN_UPLOAD_TO_S3:
        s3_key = upload_file_to_s3(local_file_path, config)
    else:
        s3_key = f"{PARTD_S3_FOLDER}/{local_file_path.name}"
        print("Skipping S3 upload.")

    conn = connect_to_snowflake(config)

    try:
        if RUN_SNOWFLAKE_TEST:
            test_snowflake_connection(conn)

        if RUN_CREATE_INFRASTRUCTURE:
            create_snowflake_database_and_schema(conn, config)
        else:
            print("Skipping database/schema creation.")

        set_snowflake_context(conn, config)

        if RUN_CREATE_STAGE:
            create_snowflake_file_format(conn)
            create_snowflake_stage(conn, config)
        else:
            print("Skipping stage creation.")

        if RUN_CREATE_RAW_TABLE:
            create_raw_partd_table(conn)
        else:
            print("Skipping RAW table creation.")

        if RUN_COPY_INTO:
            copy_into_partd_table(conn)
            validate_partd_row_count(conn)
        else:
            print("Skipping COPY INTO.")

    finally:
        conn.close()
        print("Snowflake connection closed.")

    print(f"S3 object key: {s3_key}")
    print("Script executed successfully.")


if __name__ == "__main__":
    main()