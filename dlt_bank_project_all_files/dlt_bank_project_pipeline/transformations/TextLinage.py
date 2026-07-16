import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
@dlt.table(
    name = "Linage_test",
    comment = "Testing Linage"
)
def Linage_test():
  return dlt.read("silver_customers_transformed_view_test")