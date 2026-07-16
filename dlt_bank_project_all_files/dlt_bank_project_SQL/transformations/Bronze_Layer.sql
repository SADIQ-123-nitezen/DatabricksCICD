-------------------------DATA CLEANING CUSTOMERS--------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_customers_ingestion_cleaning
(
    CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_dob EXPECT (dob IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_city EXPECT (city IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_join_date EXPECT (join_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_email EXPECT (email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') ON VIOLATION DROP ROW,
    CONSTRAINT valid_phone_number EXPECT (phone_number IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_preferred_channel EXPECT (preferred_channel IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_occupation EXPECT (occupation IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_income_range EXPECT (income_range IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_risk_segment EXPECT (risk_segment IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_gender EXPECT (gender IS NOT NULL),
    CONSTRAINT valid_status EXPECT (status IS NOT NULL)
)
COMMENT 'cleaned customers data cleaned'
TBLPROPERTIES('quality'= 'bronze')
AS 
SELECT 
    cast(t.customer_id as bigint) as customer_id,
    upper(t.name) as name,
    case when upper(t.gender) = 'M' then 'MALE'
         when upper(t.gender) = 'F' then 'FEMALE'
         else 'UNKNOWN'
         end as gender,
         upper(t.city) as city,
         cast(t.join_date as date) as join_date,
         lower(t.email) as email,
         cast(t.dob as date) as dob,
         case when 
                (t.status is null or trim(t.status) = '') 
            then 'UNKONWN'
                else  
            upper(t.status) end as status,
            cast(t.phone_number as string) as phone_number,
            upper(t.preferred_channel) as preferred_channel,
            upper(t.occupation) as occupation,
            upper(t.income_range) as income_range,
            upper(t.risk_segment) as risk_segment,
            cast(from_unixtime(unix_timestamp()) as timestamp) as batchrundate
 FROM stream(landing_customers_incremental) as t;

-------------------------DATA CLEANING ACCOUNTS--------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE bronze_accounts_ingestion_cleaning
(
    CONSTRAINT VALID_ACCOUNT_ID EXPECT (account_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT VALID_CUSTOMER_ID EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_ACCOUNT_TYPE EXPECT (UPPER(account_type) IN ('CURRENT','LOAN','SAVINGS')) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_BALANCE EXPECT (balance IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_TXN_ID EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_TXN_DATE EXPECT (txn_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_TXN_TYPE EXPECT (txn_type IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_TXN_AMOUNT EXPECT (txn_amount IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT VALID_TXN_CHANNEL EXPECT (txn_channel IS NOT NULL) ON VIOLATION DROP ROW
)                                                           
comment 'accounts customers data cleaned'
TBLPROPERTIES('quality'= 'bronze')
AS 
SELECT 
    CAST(J.account_id as bigint) as account_id,
    CAST(J.customer_id as bigint) as customer_id,
    upper(J.account_type) as account_type,
    cast(J.balance as double) as balance,
    cast(J.txn_id as bigint) as txn_id,
    cast(J.txn_date as date) as txn_date,		
    case when upper(J.txn_type) in ('DEBITT','DEBIT') then 'DEBIT'
         when upper(J.txn_type) in ('CREDIT','CREDITT') then 'CREDIT'
         else 'UNKNOWN'
         end as txn_type,
    cast(J.txn_amount as double) as txn_amount,
    upper(J.txn_channel) as txn_channel,
    cast(from_unixtime(unix_timestamp()) as timestamp) as batchrundate
FROM STREAM(landing_accounts_incremental) AS J;
