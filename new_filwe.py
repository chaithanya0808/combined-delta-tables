def processing_info_and_job_execution_delta(
    object_name: str, job_id: str, batch_status: str, 
    start_scn: Optional[int], end_scn: Optional[int], **kwargs: Any
) -> None:
    """
    Performs an upsert operation on a Delta table.

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
    
    # Path to the Delta table
    table_path = f"{FCEM_FC_SCN_DELTA_TABLE_PATH}/processing_info_and_job_execution"
    
    # Load the existing Delta table to retrieve its schema
    delta_table = DeltaTable.forPath(spark, table_path)
    table_schema = delta_table.toDF().schema
    
    # Prepare the data with explicit column names to match table schema
    data = [(job_id, job_id, object_name, start_scn, end_scn, None, None, batch_status)]
    columns = ["job_id", "batch_id", "object", "batch_start_scn", "batch_end_scn", "start_time", "end_time", "batch_status"]
    
    # Create the source DataFrame with the same schema as the table
    source_df = spark.createDataFrame(data=data, schema=table_schema) \
                     .withColumn("start_time", F.lit(start_time).cast("timestamp")) \
                     .withColumn("end_time", F.lit(end_time).cast("timestamp"))
    
    # Ensure that the source DataFrame matches the column order and types of the target table
    source_df = source_df.select([F.col(field.name).cast(field.dataType) for field in table_schema.fields])
    
    # Define the merge condition
    merge_condition = (
        "target.job_id = source.job_id AND target.batch_id = source.batch_id AND target.object = source.object"
    )
    
    # Perform the upsert operation (merge)
    delta_table.alias("target") \
               .merge(source_df.alias("source"), merge_condition) \
               .whenNotMatchedInsertAll() \
               .whenMatchedUpdate(set={
                   "start_time": F.coalesce(F.col("source.start_time"), F.col("target.start_time")),
                   "end_time": F.col("source.end_time"),
                   "batch_start_scn": F.coalesce(F.col("source.batch_start_scn"), F.col("target.batch_start_scn")),
                   "batch_end_scn": F.coalesce(F.col("source.batch_end_scn"), F.col("target.batch_end_scn")),
                   "batch_status": F.col("source.batch_status")
               }) \
               .execute()
