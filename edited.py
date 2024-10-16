def processing_info_and_job_execution_delta(object_name: str, job_id: str, batch_status: str, start_scn: Optional[int], end_scn: Optional[int],**kwargs:Any) -> None:
    """
    Performs an upsert operation on a Delta table.
    
    :param object_name: Represents the object being processed (e.g., CASE | ALERT | SMR).
    :param job_id: Unique identifier for the specific job execution.
    :param batch_status: Status of the job ('running', 'success', 'failed').
    :param start_scn: Starting SCN received from get_audit_scn method. Mandatory for inserts.
    :param end_scn: Ending SCN received from get_audit_scn method. Mandatory for inserts.
    """
    
    spark = get_or_create_spark_session()
    start_time = kwargs.get("start_time", None)
    end_time = kwargs.get("end_time", None)
    table_path = f"{FCEM_FC_SCN_DELTA_TABLE_PATH}/processing_info_and_job_execution"
    
    schema = DeltaTable.forPath(spark, table_path).toDF().schema
    data = [(job_id, job_id, object_name, start_scn, end_scn, None,None, batch_status)]
    
    # Prepare the data for insertion
    source_df = (spark.createDataFrame(data=data, schema=schema)
                 .withColumn("start_time", F.lit(start_time))
                 .withColumn("end_time", F.lit(end_time)))
    
    # Define the merge condition
    merge_condition = (
    f"target.job_id = source.job_id and target.batch_id = source.batch_id and target.object = source.object"
    f"AND target.object = '{object_name}'"
    )
    
    # Perform the upsert operation
    (
     DeltaTable.forPath(spark, table_path)
     .alias("target")
     .merge(source_df.alias("source"), merge_condition)
     .whenNotMatchedInsertAll()
     .whenMatchedUpdate(set={
         "start_time": F.coalesce("source.start_time", "target.start_time"),
         "end_time": "source.end_time",
         "batch_start_scn": F.coalesce("source.batch_start_scn", "target.batch_start_scn"),
         "batch_end_scn": F.coalesce("source.batch_end_scn", "target.batch_end_scn"),
         "batch_status": "source.batch_status",
     }
    )
    .execute()
)
