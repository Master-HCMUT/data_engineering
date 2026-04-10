from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

from utilities.parsing import parse_ts, parse_decimal, latest_record

@dp.table(name="dev.silver.sales_order_detail_clean")
@dp.expect_or_drop("valid_pk", "SalesOrderID IS NOT NULL AND SalesOrderDetailID IS NOT NULL")
@dp.expect_or_drop("valid_required_fields", """
    OrderQty IS NOT NULL AND 
    ProductID IS NOT NULL AND 
    UnitPrice IS NOT NULL
""")
def silver_sales_order_detail():
    df = (
        spark.read.table("dev.bronze.sales_order_detail")
        .select(
            F.col("SalesOrderID").cast("int").alias("SalesOrderID"),
            F.col("SalesOrderDetailID").cast("int").alias("SalesOrderDetailID"),
            F.col("CarrierTrackingNumber"),
            F.col("OrderQty").cast("int").alias("OrderQty"),
            F.col("ProductID").cast("int").alias("ProductID"),
            F.col("SpecialOfferID").cast("int").alias("SpecialOfferID"),
            parse_decimal("UnitPrice").alias("UnitPrice"),
            parse_decimal("UnitPriceDiscount").alias("UnitPriceDiscount"),
            parse_decimal("LineTotal").alias("LineTotal"),
        )
    )
    return df
