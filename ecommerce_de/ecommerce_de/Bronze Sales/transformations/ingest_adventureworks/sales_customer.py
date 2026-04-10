from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import current_timestamp, lit

from utilities.constants import BASE_VOLUME_PATH, BASE_OUTPUT_SCHEMA_PATH
from utilities.csv import read_csv_no_header

CUSTOMER_COLS = [
    "CustomerID",
    "PersonID",
    "StoreID",
    "TerritoryID",
    "AccountNumber",
    "rowguid",
    "ModifiedDate",
]
FILE_NAME = "Customer.csv"
FILE_PATH = BASE_VOLUME_PATH + "/" + FILE_NAME

@dp.table(name="dev.bronze.sales_customer")
def bronze_sales_customer():
    df = read_csv_no_header(spark, FILE_PATH, CUSTOMER_COLS)
    df = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit(FILE_PATH))
    )
    
    return df
