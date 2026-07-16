--=========== landing customers incremental table ============--
create streaming live table landing_customers_incremental 
comment 'Customers incremental data'
as 
select *, date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
from cloud_files(
    '/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema_sql/dlt_bank_project_volume_sql/customers/',
    "csv",
    map (
        "cloudFiles.inferColumnTypes", "true",
        "header", "true",
        "delimiter", ",",
        "inferschema", "true" --this is going to check csv file columns and decides which data type is suitable
        --"mergeschema", "true" --this is going to work with only new columns in the new arriving files 
    )
    );

--=========== landing accounts incremental table ============--
create streaming live table landing_accounts_incremental 
comment 'Accounts incremental data'
as 
select *, date_format(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'), 'dd-MM-yyyy HH:mm:ss') AS batch_run_date 
from cloud_files(
    '/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema_sql/dlt_bank_project_volume_sql/accounts/',
    "csv",
    map (
        "cloudFiles.inferColumnTypes", "true",
        "header", "true",
        "delimiter", ",",
        "inferschema", "true" --this is going to check csv file columns and decides which data type is suitable
        --"mergeschema", "true" --this is going to work with only new columns in the new arriving files 
    )
    );
