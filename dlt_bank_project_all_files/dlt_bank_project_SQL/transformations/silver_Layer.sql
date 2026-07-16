---------------------------CUSTOMERS DATA TRANSFORMED---------------------
create or refresh streaming table silver_customers_transformed
comment 'Silver table for customers transformed'
as 
select 
    t.* except(batchrundate),
    floor(months_between(current_date(), t.dob)/12) as customer_age,
    datediff(current_date(), t.join_date) as tenure,
    case when 
            ((t.dob < to_date('01-01-1900','dd-MM-yyyy')) OR t.dob > current_date()) 
        then 'Y'
        else
            'N'
    end dob_out_of_range_flag,
    current_timestamp() as customer_transaction_date
from STREAM(bronze_customers_ingestion_cleaning) as t;

------APPLY CHANGES CUSTOMERS-------------
CREATE STREAMING TABLE IF NOT EXISTS SILVER_CUSTOMERS_TRANSFORMED_SCD1;
APPLY CHANGES INTO SILVER_CUSTOMERS_TRANSFORMED_SCD1
FROM STREAM(silver_customers_transformed)
KEYS(CUSTOMER_ID)
SEQUENCE BY customer_transaction_date
COLUMNS * EXCEPT(customer_transaction_date)
STORED AS SCD TYPE 1; 

-------VIEW ON CUSTOMERS TRANSFORMED---------
CREATE OR REFRESH LIVE VIEW silver_Customers_transformed_VIEW
AS
SELECT * FROM LIVE.SILVER_CUSTOMERS_TRANSFORMED;


-------MATERIALIZED VIEW ON CUSTOMERS TRANSFORMED---------
CREATE OR REFRESH LIVE TABLE silver_customers_transformed_MV
AS
SELECT * FROM LIVE.SILVER_CUSTOMERS_TRANSFORMED;

---------------------------ACCOUNTS DATA TRANSFORMED---------------------
CREATE STREAMING LIVE TABLE silver_Accounts_transformed
COMMENT 'Silver table for accounts transformed'
AS
SELECT M.* EXCEPT(batchrundate),
       CASE WHEN UPPER(M.TXN_CHANNEL) IN ('ATM', 'BRANCH') THEN 'PHYSICAL' ELSE 'DIGITAL' END AS TXN_CHANNEL_TYPE,
       CASE WHEN UPPER(M.TXN_TYPE) = 'CREDIT' THEN 'IN' ELSE 'OUT' END AS TXN_DIRECTION,
       YEAR(M.TXN_DATE) AS TXN_YEAR,
       MONTH(M.TXN_DATE) AS TXN_MONTH,    
       current_timestamp() as accounts_transaction_date   
 FROM STREAM(bronze_accounts_ingestion_cleaning) AS M;


-------- ACCOUNTS TRANSFORMED SCD2---------
CREATE STREAMING LIVE TABLE SILVER_ACCOUNTS_TRANSFORMED_SCD2;

CREATE FLOW SILVER_ACCOUNTS_TRANSFORMED_SCD2_FLOW
AS AUTO CDC INTO SILVER_ACCOUNTS_TRANSFORMED_SCD2
FROM STREAM(silver_Accounts_transformed)
KEYS(account_id, customer_id, account_type)
SEQUENCE BY STRUCT(accounts_transaction_date, TXN_DATE, txn_id)
COLUMNS * EXCEPT(accounts_transaction_date)
STORED AS SCD TYPE 2;

-------VIEW ON ACCOUNTS TRANSFORMED---------
CREATE OR REFRESH LIVE VIEW silver_accounts_transformed_VIEW
AS
SELECT * FROM LIVE.SILVER_ACCOUNTS_TRANSFORMED;

-------MATERIALIZED VIEW ON ACCOUNTS TRANSFORMED---------
CREATE OR REFRESH LIVE TABLE silver_accounts_transformed_MV
AS
SELECT * FROM LIVE.SILVER_ACCOUNTS_TRANSFORMED;