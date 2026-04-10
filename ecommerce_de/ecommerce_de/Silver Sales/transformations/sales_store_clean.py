from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.parsing import parse_ts


@dp.table(name="dev.silver.sales_store_clean")
@dp.expect_or_drop("valid_pk", "StoreID IS NOT NULL")
@dp.expect_or_drop("valid_name", "StoreName IS NOT NULL")
def silver_sales_store():
    return (
        spark.read.table("dev.bronze.sales_store")
        .select(
            F.col("BusinessEntityID").cast("int").alias("StoreID"),
            F.col("Name").cast("string").alias("StoreName"),
            F.col("SalesPersonID").cast("int").alias("SalesPersonID"),
            F.col("Demographics").cast("string").alias("Demographics"),
            parse_ts("ModifiedDate").alias("ModifiedDate"),
        )
    )
