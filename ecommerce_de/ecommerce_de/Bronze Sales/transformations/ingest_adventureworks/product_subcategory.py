from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

from utilities.constants import BASE_VOLUME_PATH
from utilities.csv import read_csv_no_header

PRODUCT_SUBCATEGORY_COLS = [
    "ProductSubcategoryID",
    "ProductCategoryID",
    "Name",
    "rowguid",
    "ModifiedDate",
]

FILE_NAME = "ProductSubcategory.csv"
FILE_PATH = f"{BASE_VOLUME_PATH}/{FILE_NAME}"


@dp.table(name="dev.bronze.product_subcategory")
def bronze_product_subcategory():
    df = read_csv_no_header(spark, FILE_PATH, PRODUCT_SUBCATEGORY_COLS)
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit(FILE_PATH))
    )
