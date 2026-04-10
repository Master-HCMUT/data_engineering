from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, lit

from utilities.constants import BASE_VOLUME_PATH
from utilities.csv import read_csv_no_header

PRODUCT_CATEGORY_COLS = [
    "ProductCategoryID",
    "Name",
    "rowguid",
    "ModifiedDate",
]

FILE_NAME = "ProductCategory.csv"
FILE_PATH = f"{BASE_VOLUME_PATH}/{FILE_NAME}"


@dp.table(name="dev.bronze.product_category")
def bronze_product_category():
    df = read_csv_no_header(spark, FILE_PATH, PRODUCT_CATEGORY_COLS)
    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit(FILE_PATH))
    )
