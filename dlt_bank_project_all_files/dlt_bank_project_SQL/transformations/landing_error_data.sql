create or refresh streaming table landing_customers_incremental_error_data
comment 'Tracing all customer error data'
as
select t.* except(batch_run_date),
        'NAME IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental) AS t
WHERE t.name is null
union all
select t.* except(batch_run_date),
        'DOB IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.dob is null
union all
select t.* except(batch_run_date),
        'CITY IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.city is null
union all
select t.* except(batch_run_date),
        'JOIN DATE IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.join_date is null
union all
select t.* except(batch_run_date),
        'EMAIL IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE (t.email is null  or not 
(
    t.email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
)) 
union all
select t.* except(batch_run_date),
        'EMAIL FORMAT INCORRECT' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.email is NOT null  AND not 
(
    t.email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
)
union all
select t.* except(batch_run_date),
        'PHONE_NUMBER IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.phone_number is null
union all
select t.* except(batch_run_date),
        'PREFERRED_CHANNEL IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.preferred_channel is null      
union all
select t.* except(batch_run_date),
        'OCCUPATION IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.occupation is null         
union all
select t.* except(batch_run_date),
        'INCOME_RANGE IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.income_range is null         
union all
select t.* except(batch_run_date),
        'RISK_SEGMENT IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.risk_segment is null        
union all
select t.* except(batch_run_date),
        'GENDER IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.gender is null 
union all
select t.* except(batch_run_date),
        'STATUS IS NULL' AS ERROR_REASON,
        date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
FROM STREAM(landing_customers_incremental)  AS t
WHERE t.status is null;
