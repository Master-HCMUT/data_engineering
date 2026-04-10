from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

from utilities.parsing import parse_ts, parse_decimal, latest_record

@dp.table(name="dev.silver.sales_order_header_clean")
@dp.expect_or_drop("valid_pk", "SalesOrderID IS NOT NULL")
@dp.expect_or_drop("valid_required_fields", """
    CustomerID IS NOT NULL AND 
    OrderDate IS NOT NULL
""")
def silver_sales_order_header():
    df = (
        spark.read.table("dev.bronze.sales_order_header")
        .select(
            F.col("SalesOrderID").cast("int").alias("SalesOrderID"),
            F.col("RevisionNumber").cast("int").alias("RevisionNumber"),
            parse_ts("OrderDate").alias("OrderDate"),
            parse_ts("DueDate").alias("DueDate"),
            parse_ts("ShipDate").alias("ShipDate"),
            F.col("Status").cast("int").alias("Status"),
            F.when(F.col("OnlineOrderFlag").isin("1", "true", "TRUE"), F.lit(True))
             .when(F.col("OnlineOrderFlag").isin("0", "false", "FALSE"), F.lit(False))
             .otherwise(F.lit(None))
             .alias("OnlineOrderFlag"),
            F.col("SalesOrderNumber"),
            F.col("PurchaseOrderNumber"),
            F.col("AccountNumber"),
            F.col("CustomerID").cast("int").alias("CustomerID"),
            F.col("SalesPersonID").cast("int").alias("SalesPersonID"),
            F.col("TerritoryID").cast("int").alias("TerritoryID"),
            F.col("BillToAddressID").cast("int").alias("BillToAddressID"),
            F.col("ShipToAddressID").cast("int").alias("ShipToAddressID"),
            F.col("ShipMethodID").cast("int").alias("ShipMethodID"),
            F.col("CreditCardID").cast("int").alias("CreditCardID"),
            F.col("CreditCardApprovalCode"),
            F.col("CurrencyRateID").cast("int").alias("CurrencyRateID"),
            parse_decimal("SubTotal").alias("SubTotal"),
            parse_decimal("TaxAmt").alias("TaxAmt"),
            parse_decimal("Freight").alias("Freight"),
            parse_decimal("TotalDue").alias("TotalDue"),
            F.col("Comment"),
        )
    )
    return df
