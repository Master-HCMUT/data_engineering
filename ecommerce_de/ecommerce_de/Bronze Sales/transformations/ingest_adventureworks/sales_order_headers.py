
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import current_timestamp, lit

from utilities.constants import BASE_VOLUME_PATH, BASE_OUTPUT_SCHEMA_PATH
from utilities.csv import read_csv_no_header

SALES_ORDER_HEADER_COLS = [
    "SalesOrderID",
    "RevisionNumber",
    "OrderDate",
    "DueDate",
    "ShipDate",
    "Status",
    "OnlineOrderFlag",
    "SalesOrderNumber",
    "PurchaseOrderNumber",
    "AccountNumber",
    "CustomerID",
    "SalesPersonID",
    "TerritoryID",
    "BillToAddressID",
    "ShipToAddressID",
    "ShipMethodID",
    "CreditCardID",
    "CreditCardApprovalCode",
    "CurrencyRateID",
    "SubTotal",
    "TaxAmt",
    "Freight",
    "TotalDue",
    "Comment",
    "rowguid",
    "ModifiedDate",
]

FILE_NAME = "SalesOrderHeader.csv"
FILE_PATH = BASE_VOLUME_PATH + "/" + FILE_NAME

@dp.table(name="dev.bronze.sales_order_header")
def bronze_sales_order_detail():
    df = read_csv_no_header(spark, FILE_PATH, SALES_ORDER_HEADER_COLS)
    df = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit(FILE_PATH))
    )
    
    return df
