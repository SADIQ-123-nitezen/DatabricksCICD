######################## DATA TRANSFORMATON - CUSTOMERS TABLE ############################

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table (
    name = "silver_customers_transformed",
    comment = "Transformed customers table"
)

def silver_customers_transformed():
    df = spark.readStream.table("dlt_bank_project_catalog.dlt_bank_project_schema.bronze_customers_ingestion_cleaned")
    df = df.withColumn("customer_age", when(col("dob").isNotNull(), floor(months_between(current_date(), col("dob"))/12)).otherwise(lit(None)))
    df = df.withColumn("tenure_days", when(col("join_date").isNotNull(),datediff(current_date(),col("join_date"))).otherwise(lit(None)))
    df = df.withColumn("dob_out_of_range_flag", when((col("dob") < lit("1900-01-01")) | (col("dob") > current_date()),lit("Y")).otherwise(lit("N")))
    df = df.withColumn("transformation_date", current_timestamp())

    return df
######################## END OF DATA TRANSFORMATON - CUSTOMERS TABLE ############################

#----------------------- APPLY SD1 ---------------------------------
dlt.create_streaming_table("silver_customers_transformed_scd1")
dlt.apply_changes(
    target = "silver_customers_transformed_scd1",
    source = "silver_customers_transformed",
    keys = ["customer_id"],
    sequence_by = col("transformation_date"),
    stored_as_scd_type=1,
    except_column_list=["transformation_date"]
)
#----------------------- END OF APPLY SD1 ---------------------------------

#---------------------- VIEW --------------------------------------
@dlt.view (
    name = "silver_customers_transformed_view",
    comment = "View of silver_customers_transformed table"
)

def silver_customers_transformed():
    return spark.readStream.table("silver_customers_transformed")
#---------------------- END OF VIEW --------------------------------

#---------------------- VIEW --------------------------------------
@dlt.view (
    name = "silver_customers_transformed_view_test",
    comment = "View of silver_customers_transformed table"
)

def silver_customers_transformed_view_test():
    return spark.read.table("silver_customers_transformed")
#---------------------- END OF VIEW --------------------------------



######################## DATA TRANSFORMATON - ACCOUNTING TABLE ############################

@dlt.table(
    name = "silver_accounting_transformed",
    comment = "Transformed accounting table"
)

def silver_accounting_transformed():
    df = spark.readStream.table("dlt_bank_project_catalog.dlt_bank_project_schema.bronze_accounting_ingestion_cleaned")
    df = df.withColumn("channel_type", when(col("txn_channel").isin("ATM", "BRANCH"), "PHYSICAL").otherwise(lit("DIGITAL")))
    df = df.withColumn("txn_year", year(col("txn_date"))).withColumn("txn_month", month(col("txn_date"))).withColumn("txn_day", dayofmonth(col("txn_date")))
    df = df.withColumn("txn_direction", when(col("txn_type") == "DEBIT", lit("OUT")).otherwise(lit("IN")))
    df = df.withColumn("acc_transformation_date", current_timestamp())

    return df

#----------------------- APPLY SD2 ---------------------------------
dlt.create_streaming_table("silver_accounting_transformed_scd2")
dlt.apply_changes(
    target = "silver_accounting_transformed_scd2",
    source = "silver_accounting_transformed",
    keys = ["account_id","customer_id","account_type", "txn_id", "txn_type", "txn_channel"],
    #["account_id","customer_id","account_type", "txn_id", "txn_type", "txn_channel"]
    # THERE ARE THREE WAYS TO HANDLE THIS 
    # 1. struct
    # 2. array
    # 3. concat
    sequence_by = array (
        col("acc_transformation_date"),
        col("txn_date")
    ),
    stored_as_scd_type=2,
    except_column_list=["acc_transformation_date"]
)


#---------------------- VIEW --------------------------------------
@dlt.view (
    name = "silver_accounting_transformed_view",
    comment = "View of silver_customers_transformed table"
)

def silver_customers_transformed():
    return spark.readStream.table("silver_accounting_transformed")
#---------------------- END OF VIEW --------------------------------



