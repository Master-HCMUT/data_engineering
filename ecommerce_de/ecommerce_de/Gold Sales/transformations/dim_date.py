from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev.gold.dim_date",
    schema="""
        date_key INT,
        calendar_date DATE,
        year_number INT,
        quarter_number INT,
        month_number INT,
        month_name STRING,
        week_of_year INT,
        day_of_month INT,
        day_of_week_number INT,
        day_of_week_name STRING,
        month_start_date DATE,
        month_end_date DATE,
        is_month_end BOOLEAN,
        is_weekend BOOLEAN,
        CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
    """,
)
@dp.expect("valid_pk", "date_key IS NOT NULL")
def gold_dim_date():
    dates = (
        spark.read.table("dev.silver.sales_order_header_clean")
        .select(
            F.explode(
                F.array(
                    F.to_date("OrderDate"),
                    F.to_date("DueDate"),
                    F.to_date("ShipDate"),
                )
            ).alias("calendar_date")
        )
        .filter(F.col("calendar_date").isNotNull())
        .distinct()
    )

    return dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("calendar_date"),
        F.year("calendar_date").alias("year_number"),
        F.quarter("calendar_date").alias("quarter_number"),
        F.month("calendar_date").alias("month_number"),
        F.date_format("calendar_date", "MMMM").alias("month_name"),
        F.weekofyear("calendar_date").alias("week_of_year"),
        F.dayofmonth("calendar_date").alias("day_of_month"),
        F.dayofweek("calendar_date").alias("day_of_week_number"),
        F.date_format("calendar_date", "EEEE").alias("day_of_week_name"),
        F.trunc("calendar_date", "month").alias("month_start_date"),
        F.last_day("calendar_date").alias("month_end_date"),
        (F.col("calendar_date") == F.last_day("calendar_date")).alias("is_month_end"),
        F.dayofweek("calendar_date").isin([1, 7]).alias("is_weekend"),
    )
