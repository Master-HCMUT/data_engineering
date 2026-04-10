from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

from utilities.constants import BASE_VOLUME_PATH
from utilities.csv import read_csv_no_header

PRODUCT_COLS = [
    "ProductID",
    "Name",
    "ProductNumber",
    "MakeFlag",
    "FinishedGoodsFlag",
    "Color",
    "SafetyStockLevel",
    "ReorderPoint",
    "StandardCost",
    "ListPrice",
    "Size",
    "SizeUnitMeasureCode",
    "WeightUnitMeasureCode",
    "Weight",
    "DaysToManufacture",
    "ProductLine",
    "Class",
    "Style",
    "ProductSubcategoryID",
    "ProductModelID",
    "SellStartDate",
    "SellEndDate",
    "DiscontinuedDate",
    "rowguid",
    "ModifiedDate",
]

FILE_NAME = "Product.csv"
FILE_PATH = f"{BASE_VOLUME_PATH}/{FILE_NAME}"


@dp.table(name="dev.bronze.product")
def bronze_product():
    df = read_csv_no_header(spark, FILE_PATH, PRODUCT_COLS)
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit(FILE_PATH))
    )
