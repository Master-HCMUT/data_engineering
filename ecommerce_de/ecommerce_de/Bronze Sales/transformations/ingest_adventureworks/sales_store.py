from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit

from utilities.constants import BASE_VOLUME_PATH

STORE_COLS = [
    "BusinessEntityID",
    "Name",
    "SalesPersonID",
    "Demographics",
    "rowguid",
    "ModifiedDate",
]

FILE_NAME = "Store.csv"
FILE_PATH = f"{BASE_VOLUME_PATH}/{FILE_NAME}"


@dp.table(name="dev.bronze.sales_store")
def bronze_sales_store():
    df = (
        spark.read.text(FILE_PATH)
        .select(F.regexp_replace("value", r"&\|$", "").alias("value"))
        .select(F.split("value", r"\+\|").alias("cols"))
    )
    df_final = df.select(
        *[F.col("cols")[i].alias(STORE_COLS[i]) for i in range(len(STORE_COLS))]
    )
    return (
        df_final.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source", lit(FILE_PATH))
    )
