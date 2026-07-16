CREATE LIVE TABLE GOLD_CUST_ACC_TRANS_MV
COMMENT 'Customer Account Transactions Materialized View'
AS
SELECT 
   c.customer_id,
    c.name,
    c.gender,
    c.city,
    c.join_date,
    c.email,
    c.dob,
    c.status,
    c.phone_number,
    c.preferred_channel,	
    c.occupation,	
    c.income_range,	
    c.risk_segment,	
    c.customer_age,	
    c.tenure,
    c.dob_out_of_range_flag,	
    c.customer_transaction_date,
    a.account_id,
    a.account_type,
    a.balance,
    a.txn_id,
    a.txn_date,
    a.txn_type,
    a.txn_amount,
    a.txn_channel,
    a.TXN_CHANNEL_TYPE,
    a.TXN_DIRECTION,
    a.TXN_YEAR,
    a.TXN_MONTH,
    a.accounts_transaction_date
FROM LIVE.silver_customers_transformed as c
JOIN LIVE.silver_accounts_transformed as a
ON c.customer_id = a.customer_id