from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.parsing import parse_ts

@dp.table(name="dev.silver.sales_person_clean")
@dp.expect_or_drop("valid_pk", "BusinessEntityID IS NOT NULL")
@dp.expect_or_drop("valid_name", "FirstName IS NOT NULL OR LastName IS NOT NULL")
def silver_sales_person():
    df = (
        spark.read.table("dev.bronze.person")
        .select(
            F.col("BusinessEntityID").cast("int").alias("BusinessEntityID"),
            F.col("PersonType").cast("string").alias("PersonType"),
            F.when(F.col("NameStyle").isin("1", "true", "TRUE"), F.lit(True))
            .when(F.col("NameStyle").isin("0", "false", "FALSE"), F.lit(False))
            .otherwise(F.lit(None))
            .alias("NameStyle"),
            F.col("Title").cast("string").alias("Title"),
            F.col("FirstName").cast("string").alias("FirstName"),
            F.col("MiddleName").cast("string").alias("MiddleName"),
            F.col("LastName").cast("string").alias("LastName"),
            F.col("Suffix").cast("string").alias("Suffix"),
            F.col("EmailPromotion").cast("int").alias("EmailPromotion"),
            F.col("Demographics").cast("string").alias("Demographics"),
            parse_ts("ModifiedDate").alias("ModifiedDate"),
        )
    )
    return df
