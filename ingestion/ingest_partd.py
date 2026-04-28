import argparse
import os
from pathlib import Path

import boto3
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv


PARTD_FILE_FORMAT = "PARTD_CSV_FORMAT"
PARTD_STAGE = "PARTD_S3_STAGE"
PARTD_TABLE = "PARTD_PRESCRIBERS"

# step 1: organize config
def get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Report year to process. Example: 2023",
    )

    return parser.parse_args()


def build_runtime_config(report_year):
    return {
        "report_year": report_year,
        "source_file_name": f"partd_prescribers_{report_year}.csv",
        "compressed_file_name": f"partd_prescribers_{report_year}.csv.gz",
        "s3_folder": f"raw/year={report_year}",
    }


def load_config():
    load_dotenv()

    return {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME"),
        "LOCAL_FILE_PATH": os.getenv("LOCAL_FILE_PATH"),
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DATABASE": os.getenv("SNOWFLAKE_DATABASE"),
        "SNOWFLAKE_SCHEMA": os.getenv("SNOWFLAKE_SCHEMA"),
        "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE"),
    }


def validate_config(config):
    missing_vars = [key for key, value in config.items() if not value]

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )

# step 2: Treat csv file
def validate_local_file(base_folder, file_name):
    path = Path(base_folder) / file_name

    if not path.exists():
        raise FileNotFoundError(f"Local file not found: {path}")

    if path.suffix.lower() != ".csv":
        raise ValueError(f"Expected a CSV file, but got: {path.suffix}")

    file_size_gb = path.stat().st_size / (1024 ** 3)

    print(f"Local file found: {path}")
    print(f"File size: {file_size_gb:.2f} GB")

    return path


def add_report_year_to_csv(local_file_path, runtime):
    print("Adding REPORT_YEAR column and compressing CSV...")

    report_year = runtime["report_year"]
    output_path = local_file_path.parent / f"tmp_{runtime['compressed_file_name']}"

    chunk_size = 200_000
    first_chunk = True
    total_rows = 0

    for chunk in pd.read_csv(local_file_path, chunksize=chunk_size):
        chunk.insert(0, "REPORT_YEAR", report_year)

        chunk.to_csv(
            output_path,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False,
            compression="gzip",
        )

        total_rows += len(chunk)
        first_chunk = False
        print(f"Processed rows: {total_rows:,}")

    print(f"Temporary compressed file created: {output_path}")
    print(f"Total rows processed: {total_rows:,}")

    return output_path

# step 3: Updload zip file to S3
def upload_file_to_s3(local_file_path, config, runtime):
    file_name = runtime["compressed_file_name"]
    s3_key = f"{runtime['s3_folder']}/{file_name}"

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


def repair_athena_table(config):
    athena = boto3.client(
        "athena",
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        region_name=config["AWS_REGION"],
    )

    query = "MSCK REPAIR TABLE healthcare_portfolio.partd_prescribers_raw;"

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "healthcare_portfolio"},
        ResultConfiguration={
            "OutputLocation": f"s3://{config['S3_BUCKET_NAME']}/athena-results/"
        },
    )

    print("Athena repair started.")
    return response["QueryExecutionId"]

# step 4: Connect to Snowflake
def connect_to_snowflake(config):
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
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"Snowflake version: {version}")

    finally:
        cursor.close()

# step 5: Create Snowflake objects
def create_snowflake_database_and_schema(conn, config):
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
    cursor = conn.cursor()

    try:
        cursor.execute(f"USE ROLE {config['SNOWFLAKE_ROLE']}")
        cursor.execute(f"USE WAREHOUSE {config['SNOWFLAKE_WAREHOUSE']}")
        cursor.execute(f"USE DATABASE {config['SNOWFLAKE_DATABASE']}")
        cursor.execute(f"USE SCHEMA {config['SNOWFLAKE_SCHEMA']}")

        print(
            f"Snowflake context set: "
            f"{config['SNOWFLAKE_DATABASE']}.{config['SNOWFLAKE_SCHEMA']}"
        )

    finally:
        cursor.close()


def create_snowflake_file_format(conn, config):
    cursor = conn.cursor()

    file_format_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_FILE_FORMAT}"
    )

    try:
        cursor.execute(f"""
            CREATE OR REPLACE FILE FORMAT {file_format_name}
            TYPE = CSV
            COMPRESSION = GZIP
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
            EMPTY_FIELD_AS_NULL = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        """)

        print(f"Snowflake file format created: {file_format_name}")

    finally:
        cursor.close()


def create_snowflake_stage(conn, config, runtime):
    cursor = conn.cursor()

    stage_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_STAGE}"
    )

    file_format_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_FILE_FORMAT}"
    )

    stage_url = (
        f"s3://{config['S3_BUCKET_NAME']}/"
        f"{runtime['s3_folder']}/"
    )

    try:
        cursor.execute(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{stage_url}'
            CREDENTIALS = (
                AWS_KEY_ID = '{config["AWS_ACCESS_KEY_ID"]}'
                AWS_SECRET_KEY = '{config["AWS_SECRET_ACCESS_KEY"]}'
            )
            FILE_FORMAT = {file_format_name}
        """)

        print(f"Snowflake stage created: {stage_name}")
        print(f"Stage URL: {stage_url}")

    finally:
        cursor.close()


def create_raw_partd_table(conn, config):
    cursor = conn.cursor()

    table_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_TABLE}"
    )

    try:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                REPORT_YEAR NUMBER,
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

        print(f"RAW table verified successfully: {table_name}")

    finally:
        cursor.close()

# step 6: Delete and Copy into Snowflake
def delete_existing_year(conn, config, runtime):
    cursor = conn.cursor()

    report_year = runtime["report_year"]

    table_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_TABLE}"
    )

    try:
        cursor.execute(f"""
            DELETE FROM {table_name}
            WHERE REPORT_YEAR = {report_year}
        """)

        print(
            f"Deleted existing rows for report year "
            f"{report_year}: {cursor.rowcount:,}"
        )

    finally:
        cursor.close()


def copy_into_partd_table(conn, config):
    cursor = conn.cursor()

    table_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_TABLE}"
    )

    stage_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_STAGE}"
    )

    file_format_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_FILE_FORMAT}"
    )

    try:
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM (
                SELECT
                    $1::NUMBER,
                    $2::STRING,
                    $3::STRING,
                    $4::STRING,
                    $5::STRING,
                    $6::STRING,
                    $7::STRING,
                    $8::STRING,
                    $9::STRING,
                    $10::STRING,
                    $11::STRING,
                    $12::NUMBER,
                    $13::FLOAT,
                    $14::NUMBER,
                    $15::FLOAT,
                    $16::NUMBER,
                    $17::STRING,
                    $18::NUMBER,
                    $19::FLOAT,
                    $20::FLOAT,
                    $21::NUMBER,
                    $22::STRING,
                    $23::NUMBER
                FROM @{stage_name}
            )
            FILE_FORMAT = (FORMAT_NAME = {file_format_name})
            ON_ERROR = CONTINUE
        """)

        print(f"COPY INTO completed successfully: {table_name}")

    finally:
        cursor.close()

# step 7: Validate values
def validate_partd_row_count(conn, config):
    cursor = conn.cursor()

    table_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_TABLE}"
    )

    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        print(f"Total row count in {table_name}: {row_count:,}")

    finally:
        cursor.close()


def validate_partd_year_count(conn, config, runtime):
    cursor = conn.cursor()

    report_year = runtime["report_year"]

    table_name = (
        f"{config['SNOWFLAKE_DATABASE']}."
        f"{config['SNOWFLAKE_SCHEMA']}."
        f"{PARTD_TABLE}"
    )

    try:
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE REPORT_YEAR = {report_year}
        """)

        row_count = cursor.fetchone()[0]

        print(
            f"Row count for {report_year} in "
            f"{table_name}: {row_count:,}"
        )

    finally:
        cursor.close()


def main():
    print("Starting Medicare Part D ingestion script...")

    RUN_ADD_REPORT_YEAR_AND_COMPRESS = False
    RUN_UPLOAD_TO_S3 = False
    RUN_REPAIR_ATHENA = False

    RUN_CREATE_INFRASTRUCTURE = False
    RUN_SNOWFLAKE_TEST = True
    RUN_CREATE_STAGE = True
    RUN_CREATE_RAW_TABLE = True
    RUN_COPY_INTO = True

    args = get_args()
    runtime = build_runtime_config(args.year)

    print(f"Processing report year: {runtime['report_year']}")

    config = load_config()
    validate_config(config)

    local_file_path = validate_local_file(
        config["LOCAL_FILE_PATH"],
        runtime["source_file_name"],
    )

    if RUN_ADD_REPORT_YEAR_AND_COMPRESS:
        prepared_file_path = add_report_year_to_csv(local_file_path, runtime)
        expected_s3_file_name = runtime["compressed_file_name"]
    else:
        prepared_file_path = local_file_path
        expected_s3_file_name = local_file_path.name

    if RUN_UPLOAD_TO_S3:
        s3_key = upload_file_to_s3(prepared_file_path, config, runtime)

        if RUN_REPAIR_ATHENA:
            repair_athena_table(config)
    else:
        s3_key = f"{runtime['s3_folder']}/{expected_s3_file_name}"
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
            create_snowflake_file_format(conn, config)
            create_snowflake_stage(conn, config, runtime)
        else:
            print("Skipping stage creation.")

        if RUN_CREATE_RAW_TABLE:
            create_raw_partd_table(conn, config)
        else:
            print("Skipping RAW table creation.")

        if RUN_COPY_INTO:
            delete_existing_year(conn, config, runtime)
            copy_into_partd_table(conn, config)
            validate_partd_year_count(conn, config, runtime)
            validate_partd_row_count(conn, config)
        else:
            print("Skipping COPY INTO.")

    finally:
        conn.close()
        print("Snowflake connection closed.")

    print(f"S3 object key: {s3_key}")
    print("Script executed successfully.")


if __name__ == "__main__":
    main()