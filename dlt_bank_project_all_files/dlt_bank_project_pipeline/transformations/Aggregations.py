import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "gold_cust_acc_trns_mv_aggregated",
    comment = "Aggregated customer account transaction data",
    partition_cols = ["last_transaction_date"]
)

def gold_cust_acc_trns_mv_aggregated():
    df = dlt.read("gold_cust_acc_trns_mv")
    df = df.groupBy(
        col("customer_id"),
        col("name"),
        col("gender"),
        col("city"),
        col("status"),
        col("income_range"),
        col("risk_segment"),
        col("customer_age"),
        col("tenure_days")
    ).agg(
        countDistinct(col("account_id")).alias("Number_of_accounts"),
        countDistinct(col("account_type")).alias("Number_of_account_types"),
        round(sum(col("balance")),2).alias("total_balance"),
        countDistinct(col("txn_id")).alias("Number_of_transactions_made"),
        max(col("txn_date")).alias("last_transaction_date"),
        countDistinct(col("txn_channel")).alias("Number_of_transaction_channels"),
        max(col("txn_year")).alias("last_transaction_year"),
        max(col("txn_month")).alias("last_transaction_month")
    )

    return df
