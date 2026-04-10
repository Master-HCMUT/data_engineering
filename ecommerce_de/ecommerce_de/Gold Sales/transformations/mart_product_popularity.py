from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(name="dev.gold.mart_product_popularity")
def gold_mart_product_popularity():
    fact = spark.read.table("dev.gold.fact_sales_order_line").alias("f")
    product = spark.read.table("dev.gold.dim_product").alias("p")

    aggregated = (
        fact.join(F.broadcast(product), F.col("f.product_key") == F.col("p.product_key"), "left")
        .groupBy(
            F.col("p.product_key").alias("product_key"),
            F.col("p.ProductName").alias("product_name"),
            F.col("p.ProductCategoryName").alias("product_category_name"),
            F.col("p.ProductSubcategoryName").alias("product_subcategory_name"),
        )
        .agg(
            F.countDistinct("f.SalesOrderID").alias("distinct_order_count"),
            F.count("*").alias("line_count"),
            F.sum("f.OrderQty").alias("total_order_qty"),
            F.sum("f.GrossSalesAmount").alias("gross_sales_amount"),
            F.sum("f.DiscountAmount").alias("discount_amount"),
            F.sum("f.NetSalesAmount").alias("net_sales_amount"),
        )
    )

    quantity_window = Window.orderBy(F.col("total_order_qty").desc(), F.col("net_sales_amount").desc())
    revenue_window = Window.orderBy(F.col("net_sales_amount").desc(), F.col("total_order_qty").desc())

    return (
        aggregated.withColumn("popularity_rank_by_qty", F.dense_rank().over(quantity_window))
        .withColumn("popularity_rank_by_revenue", F.dense_rank().over(revenue_window))
    )
