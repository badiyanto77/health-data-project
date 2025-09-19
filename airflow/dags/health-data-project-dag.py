from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import boto3
import os
import time
import pandas as pd


# DAG default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def ingest_new_s3_files():
    s3_hook = S3Hook(aws_conn_id="aws_default")
    redshift_hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    bucket = "health-data-project-bucket"
    prefix = "data/"
    all_files = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)

    # Filter only CSV files
    all_csv_files = [f for f in all_files if f.endswith(".csv")]

    # Get already ingested files
    ingested_df = redshift_hook.get_pandas_df("SELECT filename FROM bronze.ingested_files")
    ingested_files = set(ingested_df["filename"].tolist())

    # Filter new files
    new_files = [f for f in all_csv_files if f.split("/")[-1] not in ingested_files]

    if not new_files:
        raise ValueError("No new files to ingest")

    # Call stored procedure for each new file
    for file_key in new_files:
        file_path = f"s3://{bucket}/{file_key}"
        redshift_hook.run(f"CALL sp_ingest_data_from_s3('{file_path}'::TEXT);")




def validate_providerinfo():
    log = LoggingMixin().log
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    query = """
    SELECT DISTINCT
        CMS_Certification_Number_CCN,
        Provider_Name,
        Provider_Address,
        City_Town,
        State,
        ZIP_Code,
        Number_of_Certified_Beds,
        Latitude,
        Longitude,
        CURRENT_TIMESTAMP AS ingestion_time
    FROM bronze.providerinfo;
    """

    df = hook.get_pandas_df(query)

    if df.empty:
        raise ValueError("Validation failed: Query returned no data from bronze.providerinfo")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()
    log.info(f"Returned columns: {df.columns.tolist()}")

    # Define expected columns
    expected_cols = [
        'cms_certification_number_ccn',
        'provider_name',
        'provider_address',
        'city_town',
        'state',
        'zip_code',
        'number_of_certified_beds',
        'latitude',
        'longitude'
    ]

    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Validation failed: Missing expected columns: {missing}")

    # Initialize error list
    errors = []

    # Validate CMS_Certification_Number_CCN: NOT NULL + UNIQUE
    cc_col = 'cms_certification_number_ccn'
    cc_nulls = df[cc_col].isnull().sum()
    cc_dupes = df[cc_col].duplicated().sum()

    if cc_nulls > 0:
        errors.append(f"{cc_nulls} NULL values in {cc_col}")
    if cc_dupes > 0:
        errors.append(f"{cc_dupes} duplicate values in {cc_col}")

    # Validate all other columns: NOT NULL
    for col in expected_cols:
        if col == cc_col:
            continue
        nulls = df[col].isnull().sum()
        if nulls > 0:
            errors.append(f"{nulls} NULL values in {col}")

    if errors:
        raise ValueError("Validation failed:\n" + "\n".join(errors))

    log.info("✅ Validation passed: All fields are NOT NULL, and CCNs are UNIQUE")
    return df

def validate_workdate():
    log = LoggingMixin().log
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    query = """
    SELECT DISTINCT 
        CAST(WorkDate AS INTEGER) AS WorkDateID,
        CAST(WorkDate AS DATE) AS WorkDate,
        EXTRACT(MONTH FROM WorkDate::DATE) AS Month,
        TO_CHAR(WorkDate::DATE, 'Month') AS MonthName,
        EXTRACT(YEAR FROM WorkDate::DATE) AS Year,
        EXTRACT(QUARTER FROM WorkDate::DATE) AS Quarter,
        CURRENT_TIMESTAMP AS updated_at
    FROM BRONZE.DailyNurseStaffing;
    """

    df = hook.get_pandas_df(query)

    if df.empty:
        raise ValueError("Validation failed: Query returned no data from BRONZE.DailyNurseStaffing")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()
    log.info(f"Returned columns: {df.columns.tolist()}")

    expected_cols = [
        'workdateid',
        'workdate',
        'month',
        'monthname',
        'year',
        'quarter',
        'updated_at'
    ]

    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Validation failed: Missing expected columns: {missing}")

    errors = []

    # Validate WorkDateID: NOT NULL + UNIQUE
    id_nulls = df['workdateid'].isnull().sum()
    id_dupes = df['workdateid'].duplicated().sum()

    if id_nulls > 0:
        errors.append(f"{id_nulls} NULL values in workdateid")
    if id_dupes > 0:
        errors.append(f"{id_dupes} duplicate values in workdateid")

    # Validate other fields: NOT NULL
    for col in expected_cols:
        if col == 'workdateid':
            continue
        nulls = df[col].isnull().sum()
        if nulls > 0:
            errors.append(f"{nulls} NULL values in {col}")

    if errors:
        raise ValueError("Validation failed:\n" + "\n".join(errors))

    log.info("✅ Validation passed: All fields are NOT NULL, and WorkDateID is UNIQUE")
    return df



def validate_fact_table():
    log = LoggingMixin().log
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    query = """
    SELECT * FROM (
        SELECT 
            PROVNUM AS CCN, 
            WorkDate AS WorkDateID, 
            MDScensus AS NumOfPatient, 
            '1' AS StaffingTypeID, 
            Hrs_RNDON_emp AS WorkHours,
            CURRENT_TIMESTAMP AS Updated_At
        FROM BRONZE.DailyNurseStaffing
        UNION
        SELECT 
            PROVNUM, 
            WorkDate, 
            MDScensus, 
            '2', 
            Hrs_RNDON_ctr, 
            CURRENT_TIMESTAMP
        FROM BRONZE.DailyNurseStaffing
        UNION
        SELECT 
            PROVNUM, 
            WorkDate,
            MDScensus, 
            '3', 
            Hrs_CNA_emp, 
            CURRENT_TIMESTAMP
        FROM BRONZE.DailyNurseStaffing
        UNION
        SELECT 
            PROVNUM, 
            WorkDate,
            MDScensus, 
            '4', 
            Hrs_CNA_ctr, 
            CURRENT_TIMESTAMP
        FROM BRONZE.DailyNurseStaffing
    ) AS unioned_data
    ORDER BY WorkDateID ASC;
    """

    df = hook.get_pandas_df(query)

    if df.empty:
        raise ValueError("Validation failed: Query returned no data from unioned DailyNurseStaffing")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()
    log.info(f"Returned columns: {df.columns.tolist()}")

    expected_cols = [
        'ccn',
        'workdateid',
        'numofpatient',
        'staffingtypeid',
        'workhours',
        'updated_at'
    ]

    missing = [col for col in expected_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Validation failed: Missing expected columns: {missing}")

    # Check for NULLs
    errors = []
    for col in expected_cols:
        nulls = df[col].isnull().sum()
        if nulls > 0:
            errors.append(f"{nulls} NULL values in {col}")

    if errors:
        raise ValueError("Validation failed:\n" + "\n".join(errors))

    log.info("✅ Validation passed: All fields are NOT NULL")
    return df



with DAG(
    dag_id="health_data_project_dag",
    default_args=default_args,
    schedule=None,  # only runs when triggered manually
    catchup=False,
    tags=["redshift", "s3", "bronze"],
) as dag:

    start = EmptyOperator(task_id="start")

    # gdrive_to_s3_task = PythonOperator(
    # task_id="gdrive_to_s3",
    # python_callable=gdrive_to_s3
    # )

    # ingest_to_redshift_task = PostgresOperator(
    #     task_id="copy_to_redshift",
    #     postgres_conn_id="redshift_default",  # Make sure this connection exists
    #     sql="CALL sp_ingest_data_from_s3();",
    # )

    ingest_to_redshift_task = PythonOperator(
        task_id="copy_to_redshift",
        python_callable=ingest_new_s3_files,
    )

    transform_dim_provider_silver = PostgresOperator(
        task_id="transform_dim_provider_silver",
        postgres_conn_id="redshift_default",  # Make sure this connection exists
        sql="CALL sp_generate_silver_provider_dim();",
    )

    validate_providerinfo_task = PythonOperator(
        task_id="validate_providerinfo",
        python_callable=validate_providerinfo
    )

    validate_workdate_task = PythonOperator(
        task_id="validate_dailynursestaffing",
        python_callable=validate_workdate
    )

    validate_fact_table_task = PythonOperator(
        task_id="validate_fact_table",
        python_callable= validate_fact_table
    )
   
   
    transform_fact_table_silver = PostgresOperator(
        task_id="transform_fact_table_silver",
        postgres_conn_id="redshift_default",  # Make sure this connection exists
        sql="CALL sp_generate_silver_fact_table();",
    )

    transform_dim_staffingtype_silver = PostgresOperator(
        task_id="transform_dim_staffingtype_silver",
        postgres_conn_id="redshift_default",  # Make sure this connection exists
        sql="CALL sp_generate_silver_staffingtype_dim();",
    )


   

    transform_dim_workdate_silver = PostgresOperator(
        task_id="transform_dim_workdate_silver",
        postgres_conn_id="redshift_default",  # Make sure this connection exists
        sql="CALL sp_generate_silver_workdate_dim();",
    )

    transform_provider_staffing_utilization_metric_gold = PostgresOperator(
        task_id="transform_provider_staffing_utilization_metric_gold",
        postgres_conn_id="redshift_default",  # Make sure this connection exists
        sql="CALL sp_generate_gold_provider_staffing_utilization_metric();",
    )

    end = EmptyOperator(task_id="end")


# Create a dummy "validation_gate" that only proceeds if both validations succeed
validation_gate_01 = EmptyOperator(
    task_id="validation_gate_01",
    trigger_rule=TriggerRule.ALL_SUCCESS
)
validation_gate_02 = EmptyOperator(
    task_id="validation_gate_02",
    trigger_rule=TriggerRule.ALL_SUCCESS
)


# DAG structure
start >>  ingest_to_redshift_task 

# # Run both validations in parallel
ingest_to_redshift_task >> [validate_providerinfo_task, validate_workdate_task,validate_fact_table_task]



# # Connect validations to the gate
[validate_providerinfo_task, validate_workdate_task, validate_fact_table_task] >> validation_gate_01

# # Downstream transformations only run if gate passes
validation_gate_01 >> [
    transform_dim_provider_silver,
    transform_fact_table_silver,
    transform_dim_staffingtype_silver,
    transform_dim_workdate_silver
] >> validation_gate_02 >> transform_provider_staffing_utilization_metric_gold >> end
