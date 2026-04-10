from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="dev.gold.mart_monthly_territory_sales")
def gold_mart_monthly_territory_sales():
    fact = spark.read.table("dev.gold.fact_sales_order_line").alias("f")
    date_dim = spark.read.table("dev.gold.dim_date").alias("d")
    territory_dim = spark.read.table("dev.gold.dim_sales_territory").alias("t")

    aggregated = (
        fact.join(date_dim, F.col("f.order_date_key") == F.col("d.date_key"), "left")
        .join(territory_dim, F.col("f.territory_key") == F.col("t.territory_key"), "left")
        .groupBy(
            F.col("d.year_number").alias("year_number"),
            F.col("d.month_number").alias("month_number"),
            F.col("d.month_name").alias("month_name"),
            F.col("d.month_start_date").alias("month_start_date"),
            F.col("t.territory_key").alias("territory_key"),
            F.col("t.TerritoryName").alias("territory_name"),
            F.col("t.CountryRegionCode").alias("country_region_code"),
            F.col("t.TerritoryGroup").alias("territory_group"),
        )
        .agg(
            F.countDistinct("f.SalesOrderID").alias("order_count"),
            F.countDistinct("f.customer_key").alias("customer_count"),
            F.sum("f.OrderQty").alias("total_order_qty"),
            F.sum("f.GrossSalesAmount").alias("gross_revenue"),
            F.sum("f.DiscountAmount").alias("discount_amount"),
            F.sum("f.NetSalesAmount").alias("net_revenue"),
        )
    )

    return aggregated.select(
        F.col("year_number"),
        F.col("month_number"),
        F.col("month_name"),
        F.col("month_start_date"),
        F.col("territory_key"),
        F.col("territory_name"),
        F.col("country_region_code"),
        F.col("territory_group"),
        F.col("order_count"),
        F.col("customer_count"),
        F.col("total_order_qty"),
        F.col("gross_revenue"),
        F.col("discount_amount"),
        F.col("net_revenue"),
        F.when(F.col("order_count") > 0, F.col("net_revenue") / F.col("order_count"))
        .otherwise(F.lit(None))
        .cast("decimal(19,4)")
        .alias("avg_order_value"),
    )
