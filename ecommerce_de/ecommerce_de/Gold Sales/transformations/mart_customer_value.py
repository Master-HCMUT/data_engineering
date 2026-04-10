from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(name="dev.gold.mart_customer_value")
def gold_mart_customer_value():
    fact = spark.read.table("dev.gold.fact_sales_order_line").alias("f")
    customer_dim = spark.read.table("dev.gold.dim_customer").alias("c")
    territory_dim = spark.read.table("dev.gold.dim_sales_territory").alias("t")
    date_dim = spark.read.table("dev.gold.dim_date").alias("d")

    aggregated = (
        fact.join(customer_dim, F.col("f.customer_key") == F.col("c.customer_key"), "left")
        .join(territory_dim, F.col("f.territory_key") == F.col("t.territory_key"), "left")
        .join(date_dim, F.col("f.order_date_key") == F.col("d.date_key"), "left")
        .groupBy(
            F.col("c.customer_key").alias("customer_key"),
            F.col("c.CustomerName").alias("customer_name"),
            F.col("c.CustomerType").alias("customer_type"),
            F.col("c.StoreName").alias("store_name"),
            F.col("t.territory_key").alias("territory_key"),
            F.col("t.TerritoryName").alias("territory_name"),
            F.col("t.TerritoryGroup").alias("territory_group"),
        )
        .agg(
            F.countDistinct("f.SalesOrderID").alias("order_count"),
            F.sum("f.OrderQty").alias("total_order_qty"),
            F.sum("f.GrossSalesAmount").alias("gross_revenue"),
            F.sum("f.DiscountAmount").alias("discount_amount"),
            F.sum("f.NetSalesAmount").alias("net_revenue"),
            F.min("d.calendar_date").alias("first_order_date"),
            F.max("d.calendar_date").alias("last_order_date"),
        )
    )

    ranking_window = Window.orderBy(F.col("net_revenue").desc(), F.col("order_count").desc())

    return (
        aggregated.select(
            F.col("customer_key"),
            F.col("customer_name"),
            F.col("customer_type"),
            F.col("store_name"),
            F.col("territory_key"),
            F.col("territory_name"),
            F.col("territory_group"),
            F.col("order_count"),
            F.col("total_order_qty"),
            F.col("gross_revenue"),
            F.col("discount_amount"),
            F.col("net_revenue"),
            F.when(F.col("order_count") > 0, F.col("net_revenue") / F.col("order_count"))
            .otherwise(F.lit(None))
            .cast("decimal(19,4)")
            .alias("avg_order_value"),
            F.col("first_order_date"),
            F.col("last_order_date"),
        )
        .withColumn("rank_by_customer_value", F.dense_rank().over(ranking_window))
    )
