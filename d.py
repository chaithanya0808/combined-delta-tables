SELECT 
    t1.object_id, 
    t2.batch_id, 
    COUNT(*) AS record_count
FROM 
    delta.`<table_1_path>` AS t1
JOIN 
    delta.`<table_2_path>` AS t2
ON 
    t1.job_id = t2.job_id  -- Assuming `job_id` is the common key for the join
GROUP BY 
    t1.object_id, 
    t2.batch_id
