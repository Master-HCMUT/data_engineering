from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev.gold.dim_sales_territory",
    schema="""
        territory_key INT,
        TerritoryID INT,
        TerritoryName STRING,
        CountryRegionCode STRING,
        TerritoryGroup STRING,
        SalesYTD DECIMAL(19,4),
        SalesLastYear DECIMAL(19,4),
        CostYTD DECIMAL(19,4),
        CostLastYear DECIMAL(19,4),
        CONSTRAINT pk_dim_sales_territory PRIMARY KEY (territory_key)
    """,
)
@dp.expect("valid_pk", "territory_key IS NOT NULL")
def gold_dim_sales_territory():
    return spark.read.table("dev.silver.sales_territory_clean").select(
        F.col("TerritoryID").alias("territory_key"),
        F.col("TerritoryID"),
        F.col("TerritoryName"),
        F.col("CountryRegionCode"),
        F.col("TerritoryGroup"),
        F.col("SalesYTD"),
        F.col("SalesLastYear"),
        F.col("CostYTD"),
        F.col("CostLastYear"),
    )
