import os
import boto3
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path


def load_config():
    """Load environment variables from .env file."""
    load_dotenv()

    return {
        # AWS
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
        "S3_FOLDER": os.getenv("S3_FOLDER"),

        # Local file
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

def validate_local_file(file_path):
    """Validate that the local source file exists and is a CSV."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Local file not found: {file_path}")

    if path.suffix.lower() != ".csv":
        raise ValueError(f"Expected a CSV file, but got: {path.suffix}")

    file_size_gb = path.stat().st_size / (1024 ** 3)

    print(f"Local file found: {path}")
    print(f"File size: {file_size_gb:.2f} GB")

    return path        

def upload_file_to_s3(local_file_path, config):
    """Upload the local CSV file to the configured S3 bucket and folder."""
    file_name = local_file_path.name
    s3_key = f"{config['S3_FOLDER']}/{file_name}"

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
    """Run simple test query."""
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
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}")

        print("Snowflake database and schema verified successfully.")

    finally:
        cursor.close()        

def set_snowflake_context(conn, config):
    """Set active Snowflake warehouse, database, schema, and role."""
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
    """Create CSV file format for staged CMS Part D data."""
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE OR REPLACE FILE FORMAT PARTD_CSV_FORMAT
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        """)

        print("Snowflake file format created successfully.")

    finally:
        cursor.close()

def create_snowflake_stage(conn, config):
    """Create external stage pointing to the S3 folder."""
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            CREATE OR REPLACE STAGE PARTD_S3_STAGE
            URL = 's3://{config["S3_BUCKET_NAME"]}/{config["S3_FOLDER"]}/'
            CREDENTIALS = (
                AWS_KEY_ID = '{config["AWS_ACCESS_KEY_ID"]}'
                AWS_SECRET_KEY = '{config["AWS_SECRET_ACCESS_KEY"]}'
            )
            FILE_FORMAT = PARTD_CSV_FORMAT
        """)
        print("Snowflake external stage created successfully.")

    finally:
        cursor.close()       

def create_raw_partd_table(conn):
    """Create RAW.PARTD_PRESCRIBERS table with explicit CMS Part D schema."""
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE OR REPLACE TABLE PARTD_PRESCRIBERS (
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

        print("RAW table PARTD_PRESCRIBERS created successfully.")

    finally:
        cursor.close()   

def copy_into_partd_table(conn):
    """Load staged CMS Part D CSV data into RAW.PARTD_PRESCRIBERS."""
    cursor = conn.cursor()

    try:
        cursor.execute("""
            COPY INTO PARTD_PRESCRIBERS
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
                FROM @PARTD_S3_STAGE
            )
            FILE_FORMAT = (FORMAT_NAME = PARTD_CSV_FORMAT)
            ON_ERROR = CONTINUE
        """)

        print("COPY INTO PARTD_PRESCRIBERS completed successfully.")

    finally:
        cursor.close()


def validate_partd_row_count(conn):
    """Validate row count after loading data."""
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM PARTD_PRESCRIBERS")
        row_count = cursor.fetchone()[0]

        print(f"PARTD_PRESCRIBERS row count: {row_count:,}")

    finally:
        cursor.close()        


def main():
    print("Starting Part D ingestion script...")

    RUN_UPLOAD_TO_S3 = False
    RUN_CREATE_INFRASTRUCTURE = False
    RUN_SNOWFLAKE_TEST = False
    RUN_CREATE_STAGE = False
    RUN_CREATE_RAW_TABLE = False
    RUN_COPY_INTO = False 

    config = load_config()
    validate_config(config)

    local_file_path = validate_local_file(config["LOCAL_FILE_PATH"])

    if RUN_UPLOAD_TO_S3:
        s3_key = upload_file_to_s3(local_file_path, config)
    else:
        file_name = local_file_path.name
        s3_key = f"{config['S3_FOLDER']}/{file_name}"
        print("Skipping S3 upload.")

    conn = connect_to_snowflake(config)

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

    conn.close()

    print("Script executed successfully.")


if __name__ == "__main__":
    main()