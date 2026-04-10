from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

def parse_ts(c: str):
    return F.coalesce(
        F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col(c)),
    )

def parse_decimal(c: str, p: int = 19, s: int = 4):
    return F.col(c).cast(DecimalType(p, s))

def latest_record(df, partition_cols, date_col):
    w = Window.partitionBy(*partition_cols).orderBy(F.col(date_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )