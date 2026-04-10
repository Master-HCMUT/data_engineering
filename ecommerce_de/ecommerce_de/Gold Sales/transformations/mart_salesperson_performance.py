from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F


@dp.table(name="dev.gold.mart_salesperson_performance")
def gold_mart_salesperson_performance():
    fact = (
        spark.read.table("dev.gold.fact_sales_order_line")
        .filter(F.col("salesperson_key").isNotNull())
        .alias("f")
    )
    salesperson_dim = spark.read.table("dev.gold.dim_salesperson").alias("sp")
    territory_dim = spark.read.table("dev.gold.dim_sales_territory").alias("t")

    aggregated = (
        fact.join(salesperson_dim, F.col("f.salesperson_key") == F.col("sp.salesperson_key"), "left")
        .join(territory_dim, F.col("f.territory_key") == F.col("t.territory_key"), "left")
        .groupBy(
            F.col("sp.salesperson_key").alias("salesperson_key"),
            F.col("sp.SalesPersonName").alias("salesperson_name"),
            F.col("sp.PersonType").alias("person_type"),
            F.col("t.territory_key").alias("territory_key"),
            F.col("t.TerritoryName").alias("territory_name"),
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

    ranking_window = Window.orderBy(F.col("net_revenue").desc(), F.col("order_count").desc())

    return (
        aggregated.select(
            F.col("salesperson_key"),
            F.col("salesperson_name"),
            F.col("person_type"),
            F.col("territory_key"),
            F.col("territory_name"),
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
        .withColumn("rank_by_revenue", F.dense_rank().over(ranking_window))
    )
