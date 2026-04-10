from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

from utilities.parsing import parse_ts, parse_decimal, latest_record

@dp.table(name="dev.silver.sales_customer_clean")
@dp.expect_or_drop("valid_pk", "CustomerID IS NOT NULL")
@dp.expect_or_drop("valid_customer_type", """
    StoreID IS NOT NULL OR 
    PersonID IS NOT NULL
""")
def silver_customer():
    df = (
        spark.read.table("dev.bronze.sales_customer")
        .select(
            F.col("CustomerID").cast("int"),
            F.col("PersonID").cast("int"),
            F.col("StoreID").cast("int"),
            F.col("TerritoryID").cast("int"),
            F.col("AccountNumber").cast("string")
        )
    )
    return df
