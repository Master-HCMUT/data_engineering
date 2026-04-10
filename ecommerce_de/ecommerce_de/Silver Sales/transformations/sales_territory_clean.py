from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.parsing import parse_decimal, parse_ts


@dp.table(name="dev.silver.sales_territory_clean")
@dp.expect_or_drop("valid_pk", "TerritoryID IS NOT NULL")
@dp.expect_or_drop("valid_name", "TerritoryName IS NOT NULL")
def silver_sales_territory():
    return (
        spark.read.table("dev.bronze.sales_territory")
        .select(
            F.col("TerritoryID").cast("int").alias("TerritoryID"),
            F.col("Name").cast("string").alias("TerritoryName"),
            F.col("CountryRegionCode").cast("string").alias("CountryRegionCode"),
            F.col("Group").cast("string").alias("TerritoryGroup"),
            parse_decimal("SalesYTD").alias("SalesYTD"),
            parse_decimal("SalesLastYear").alias("SalesLastYear"),
            parse_decimal("CostYTD").alias("CostYTD"),
            parse_decimal("CostLastYear").alias("CostLastYear"),
            parse_ts("ModifiedDate").alias("ModifiedDate"),
        )
    )
