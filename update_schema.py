def overwrite_delta_table_with_new_schema(
    object_name: str, job_id: str, batch_status: str, 
    start_scn: Optional[int], end_scn: Optional[int], **kwargs: Any
) -> None:
    """
    Overwrites the Delta table schema and upserts data.

    :param object_name: Represents the object being processed (e.g., CASE | ALERT | SMR).
    :param job_id: Unique identifier for the specific job execution.
    :param batch_status: Status of the job ('running', 'success', 'failed').
    :param start_scn: Starting SCN received from get_audit_scn method. Mandatory for inserts.
    :param end_scn: Ending SCN received from get_audit_scn method. Mandatory for inserts.
    """
    
    # Create or get the Spark session
    spark = get_or_create_spark_session()
    
    # Extract optional parameters
    start_time = kwargs.get("start_time", None)
    end_time = kwargs.get("end_time", None)
    
    # Define the table path
    table_path = f"{FCEM_FC_SCN_DELTA_TABLE_PATH}/processing_info_and_job_execution"
    
    # Define the updated schema (with start_time and end_time)
    updated_schema = StructType([
        StructField("job_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("object", StringType(), True),
        StructField("batch_start_scn", LongType(), True),
        StructField("batch_end_scn", LongType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("batch_status", StringType(), True)
    ])
    
    # Prepare the data with the updated schema
    data = [(job_id, job_id, object_name, start_scn, end_scn, start_time, end_time, batch_status)]
    
    # Create a DataFrame using the updated schema
    source_df = spark.createDataFrame(data, schema=updated_schema)
    
    # Write the DataFrame to Delta with schema overwrite
    source_df.write \
             .format("delta") \
             .mode("overwrite") \
             .option("overwriteSchema", "true") \
             .save(table_path)

    print(f"Schema for Delta table at {table_path} has been overwritten.")
