import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table (
    name = "gold_cust_acc_trns_mv",
    comment = "Customer Account Transaction Materialized View",
)
def gold_cust_acc_trns_mv():
    customers = spark.read.table("dlt_bank_project_catalog.dlt_bank_project_schema.silver_customers_transformed").drop("_rescued_data").alias("c")
    account_tx = spark.read.table("dlt_bank_project_catalog.dlt_bank_project_schema.silver_accounting_transformed").drop("_rescued_data").alias("a")

    joined = customers.join(
        account_tx,
        on = (col("c.customer_id") == col("a.customer_id")),
        how = "inner"
        ).select(
            "c.*",
            col("a.account_id"),
            col("a.account_type"),
            col("a.balance"),
            col("a.txn_id"),
            col("a.txn_date"),
            col("a.txn_type"),
            col("a.txn_amount"),
            col("a.txn_channel"),
            col("a.channel_type"),
            col("a.txn_year"),
            col("a.txn_month"),
            col("a.txn_day"),
            col("a.txn_direction"),
            col("a.acc_transformation_date")
            )

    return joined
