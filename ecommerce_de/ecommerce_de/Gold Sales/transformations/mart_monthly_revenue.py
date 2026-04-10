from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="dev.gold.mart_monthly_revenue")
def gold_mart_monthly_revenue():
    fact = spark.read.table("dev.gold.fact_sales_order_line").alias("f")
    date_dim = spark.read.table("dev.gold.dim_date").alias("d")

    return (
        fact.join(date_dim, F.col("f.order_date_key") == F.col("d.date_key"), "left")
        .groupBy(
            F.col("d.year_number").alias("year_number"),
            F.col("d.month_number").alias("month_number"),
            F.col("d.month_name").alias("month_name"),
            F.col("d.month_start_date").alias("month_start_date"),
        )
        .agg(
            F.countDistinct("f.SalesOrderID").alias("order_count"),
            F.sum("f.OrderQty").alias("total_order_qty"),
            F.sum("f.GrossSalesAmount").alias("gross_revenue"),
            F.sum("f.DiscountAmount").alias("discount_amount"),
            F.sum("f.NetSalesAmount").alias("net_revenue"),
        )
    )
