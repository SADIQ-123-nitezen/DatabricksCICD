#################################### Customers 2023 Data Ingestion #########################################


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

## Defining Blue Print of Data Frame
customer_schema = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("dob", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("city", StringType(), True),
        StructField("join_date", DateType(), True),
        StructField("status", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("preferred_channel", StringType(), True),
        StructField("occupation", StringType(),	True),	
        StructField("income_range", StringType(), True),
        StructField("risk_segment", StringType(), True)
    ]
)

@dlt.table (
    name = "landing_customers_data_incremental",
    comment = "This table contains the incremental data for customers"
)

def landing_customers_data_incremental() :
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(customer_schema)
        .load("/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema/dlt_bank_project_volume/customers/")
    )

#################################### END of Customers 2023 Data Ingestion #########################################


######################################### Accounts 2023 Data Ingestion ############################################

accounts_schema = StructType (
    [
        StructField("account_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("account_type", StringType(), True),
        StructField("balance", DoubleType(), True),
        StructField("txn_id", LongType(), True),
        StructField("txn_date", DateType(), True),
        StructField("txn_type",StringType(), True),
        StructField("txn_amount", DoubleType(), True),
        StructField("txn_channel", StringType(), True)
    ]
)

@dlt.table (
    name = "landing_accounting_data_incremental",
    comment = "This table contains the incremental data for customers"
)

def landing_accounting_data_incremental() :
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(accounts_schema)
        .load("/Volumes/dlt_bank_project_catalog/dlt_bank_project_schema/dlt_bank_project_volume/accounts/")
    )
