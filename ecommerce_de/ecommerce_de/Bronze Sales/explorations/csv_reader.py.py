# Databricks notebook source
# Read raw csv file without header
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/dev/raw/oltp/SalesOrderDetail.csv")

display(df)

# COMMAND ----------

SALES_ORDER_DETAIL_COLS = [
    "SalesOrderID",
    "SalesOrderDetailID",
    "CarrierTrackingNumber",
    "OrderQty",
    "ProductID",
    "SpecialOfferID",
    "UnitPrice",
    "UnitPriceDiscount",
    "LineTotal",
    "rowguid",
    "ModifiedDate",
]

df = spark.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", "\t") \
    .load("/Volumes/dev/raw/oltp/SalesOrderDetail.csv")

df = df.toDF(*SALES_ORDER_DETAIL_COLS)



# COMMAND ----------

display(df)
print(df.columns)
print(len(df.columns))

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "false") \
    .option("delimiter", "|") \
    .load("/Volumes/dev/raw/oltp/Person.csv")

# COMMAND ----------

print(df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC rdd

# COMMAND ----------

df_raw = spark.read.text("/Volumes/dev/raw/oltp/Person.csv")

df_raw

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df = (
    spark.read.text("/Volumes/dev/raw/oltp/Person.csv")
    .select(F.regexp_replace("value", r"&\|$", "").alias("value"))
    .select(F.split("value", r"\+\|").alias("cols"))
)

# COMMAND ----------

columns = [
    "BusinessEntityID",
    "PersonType",
    "NameStyle",
    "Title",
    "FirstName",
    "MiddleName",
    "LastName",
    "Suffix",
    "EmailPromotion",
    "AdditionalContactInfo",
    "Demographics",
    "rowguid",
    "ModifiedDate"
]
df_final = df.select(
    *[F.col("cols")[i].alias(columns[i]) for i in range(len(columns))]
)

# COMMAND ----------

display(df_final)

# COMMAND ----------

