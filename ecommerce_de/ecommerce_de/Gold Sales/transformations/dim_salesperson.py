from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="dev.gold.dim_salesperson",
    schema="""
        salesperson_key INT,
        SalesPersonID INT,
        SalesPersonName STRING,
        PersonType STRING,
        EmailPromotion INT,
        CONSTRAINT pk_dim_salesperson PRIMARY KEY (salesperson_key)
    """,
)
@dp.expect("valid_pk", "salesperson_key IS NOT NULL")
def gold_dim_salesperson():
    sales_people = (
        spark.read.table("dev.silver.sales_order_header_clean")
        .select(F.col("SalesPersonID").alias("salesperson_key"))
        .filter(F.col("salesperson_key").isNotNull())
        .distinct()
        .alias("sp")
    )
    person = spark.read.table("dev.silver.sales_person_clean").alias("p")

    return sales_people.join(
        person,
        F.col("sp.salesperson_key") == F.col("p.BusinessEntityID"),
        "left",
    ).select(
        F.col("sp.salesperson_key"),
        F.col("p.BusinessEntityID").alias("SalesPersonID"),
        F.trim(
            F.concat_ws(
                " ",
                F.col("p.Title"),
                F.col("p.FirstName"),
                F.col("p.MiddleName"),
                F.col("p.LastName"),
                F.col("p.Suffix"),
            )
        ).alias("SalesPersonName"),
        F.col("p.PersonType").alias("PersonType"),
        F.col("p.EmailPromotion").alias("EmailPromotion"),
    )
